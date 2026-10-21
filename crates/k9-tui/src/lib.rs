//! `k9 run --tui`: a live dashboard in the terminal while a test runs.
//!
//! [`Dashboard`] holds what's shown (fed by the engine's [`Progress`], one
//! [`LiveFrame`] a second, and [`RunEvent`]s) and draws it; [`run`] owns the
//! terminal (alternate screen, raw mode) until the run ends, and turns keys
//! into [`RunControl`] calls: `p` pauses or resumes, `+`/`-` change the VUs,
//! `q` (or Ctrl-C) stops, a second time exits at once.
//!
//! [`Progress`]: k9_core::engine::Progress
//! [`LiveFrame`]: k9_core::engine::LiveFrame

mod dashboard;
mod theme;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use k9_core::engine::{RunControl, RunEvent};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

pub use dashboard::{Dashboard, Header, Point};
pub use theme::{ColorDepth, Theme, ThemeName};

/// What a key asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Stop the run gracefully (the second time: exit at once).
    Stop,
    TogglePause,
    /// Change the externally-controlled scenario's VUs by this many.
    Scale(i32),
}

pub fn key_action(key: KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Some(Action::Stop),
        KeyCode::Char('q') | KeyCode::Esc => Some(Action::Stop),
        KeyCode::Char('p') | KeyCode::Char(' ') => Some(Action::TogglePause),
        KeyCode::Char('+') | KeyCode::Char('=') | KeyCode::Up => Some(Action::Scale(1)),
        KeyCode::Char('-') | KeyCode::Down => Some(Action::Scale(-1)),
        _ => None,
    }
}

/// How the dashboard runs.
pub struct Options {
    pub header: Header,
    pub theme: Theme,
    /// The exit code for a second stop, which exits at once.
    pub force_exit_code: i32,
}

/// How often the screen redraws between frames (for the clock and keys).
const REDRAW: Duration = Duration::from_millis(250);

/// Shows the dashboard until `done` is cancelled (the run ended), then gives
/// the terminal back as it was. `notes` are extra log lines (e.g. warnings
/// about the load generator itself). Subscribe to `events` before the run
/// starts so no first error is missed.
pub async fn run(
    control: RunControl,
    options: Options,
    mut events: broadcast::Receiver<RunEvent>,
    mut notes: mpsc::UnboundedReceiver<String>,
    done: CancellationToken,
) -> std::io::Result<()> {
    let mut dashboard = Dashboard::new(options.header, options.theme);
    let mut terminal = ratatui::try_init()?;
    let (keys_tx, mut keys) = mpsc::unbounded_channel();
    let reading = Arc::new(AtomicBool::new(true));
    let reader = std::thread::spawn({
        let reading = reading.clone();
        move || {
            while reading.load(Ordering::Relaxed) {
                match event::poll(Duration::from_millis(100)) {
                    Ok(true) => {
                        if let Ok(Event::Key(key)) = event::read()
                            && key.kind == KeyEventKind::Press
                            && keys_tx.send(key).is_err()
                        {
                            return;
                        }
                    }
                    Ok(false) => {}
                    Err(_) => return,
                }
            }
        }
    });
    let mut progress = control.progress();
    let mut frames = control.frames();
    let mut redraw = tokio::time::interval(REDRAW);
    let result = loop {
        tokio::select! {
            _ = done.cancelled() => break Ok(()),
            _ = redraw.tick() => {}
            Ok(()) = progress.changed() => {
                dashboard.apply_progress(*progress.borrow_and_update());
            }
            Ok(()) = frames.changed() => {
                let frame = frames.borrow_and_update().clone();
                dashboard.apply_frame(&frame);
            }
            event = events.recv() => match event {
                Ok(event) => dashboard.log_event(event),
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => {}
            },
            Some(note) = notes.recv() => dashboard.note(format!("WARN {note}")),
            Some(key) = keys.recv() => {
                if let Some(action) = key_action(key) {
                    act(action, &control, &mut dashboard, options.force_exit_code);
                }
            }
        }
        dashboard.set_paused(control.status().paused);
        if let Err(err) = terminal.draw(|f| dashboard.render(f)) {
            break Err(err);
        }
    };
    reading.store(false, Ordering::Relaxed);
    let _ = reader.join();
    ratatui::try_restore()?;
    result
}

fn act(action: Action, control: &RunControl, dashboard: &mut Dashboard, force_exit_code: i32) {
    match action {
        Action::Stop if dashboard.is_stopping() => {
            let _ = ratatui::try_restore();
            eprintln!("forced exit");
            std::process::exit(force_exit_code);
        }
        Action::Stop => {
            dashboard.set_stopping();
            dashboard.note("stopping the test run; press q again to exit now");
            control.stop();
        }
        Action::TogglePause => {
            let result = if dashboard.is_paused() {
                control.resume()
            } else {
                control.pause()
            };
            match result {
                Ok(()) => dashboard.set_paused(!dashboard.is_paused()),
                Err(err) => dashboard.note(format!("can't pause: {err}")),
            }
        }
        Action::Scale(by) => {
            let vus = dashboard.vus().saturating_add_signed(by).max(1);
            match control.scale(vus) {
                Ok(()) => dashboard.note(format!("VUs → {vus}")),
                Err(err) => dashboard.note(format!("can't change the VUs: {err}")),
            }
        }
    }
}
