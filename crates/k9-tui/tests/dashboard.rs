//! The dashboard rendered into a test terminal: numbers, colours, charts'
//! history, the log, and keys.

use std::time::Duration;

use k9_core::engine::{LiveCheck, LiveFrame, Progress, RunEvent, ThresholdOutcome};
use k9_metrics::Stats;
use k9_tui::{Action, ColorDepth, Dashboard, Header, Theme, ThemeName, key_action};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::style::Color;

fn header() -> Header {
    Header {
        script: "script.py".into(),
        max_vus: 20,
        max_duration: Duration::from_secs(60),
    }
}

fn dashboard() -> Dashboard {
    Dashboard::new(
        header(),
        Theme::new(ThemeName::Brand, ColorDepth::TrueColor),
    )
}

fn frame(secs: u64, reqs: f64, failed: (u64, u64), p95: f64) -> LiveFrame {
    let trend = |p95: f64| Stats::Trend {
        count: 10,
        avg: p95 / 2.0,
        min: 1.0,
        med: p95 / 2.0,
        max: p95 * 2.0,
        p90: p95,
        p95,
        p99: p95 * 1.5,
    };
    LiveFrame {
        elapsed: Duration::from_secs(secs),
        last_second: vec![
            ("http_req_duration".into(), trend(p95)),
            (
                "http_req_failed".into(),
                Stats::Rate {
                    trues: failed.0,
                    total: failed.1,
                },
            ),
            ("http_reqs".into(), Stats::Counter { sum: reqs }),
            ("iterations".into(), Stats::Counter { sum: reqs / 2.0 }),
        ],
        totals: Vec::new(),
        checks: vec![
            LiveCheck {
                name: "status is 200".into(),
                passes: 99,
                fails: 1,
            },
            LiveCheck {
                name: "fast".into(),
                passes: 50,
                fails: 50,
            },
        ],
        thresholds: vec![
            ThresholdOutcome {
                selector: "http_req_duration".into(),
                expression: "p(95)<500".into(),
                observed: 120.0,
                passed: true,
            },
            ThresholdOutcome {
                selector: "http_req_failed".into(),
                expression: "rate<0.01".into(),
                observed: 0.02,
                passed: false,
            },
        ],
    }
}

fn progress(secs: u64, vus: u32) -> Progress {
    Progress {
        elapsed: Duration::from_secs(secs),
        active_vus: vus,
        max_vus: 20,
        iterations_complete: 1234,
        iterations_interrupted: 0,
    }
}

fn render(dashboard: &Dashboard) -> Buffer {
    let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
    terminal.draw(|f| dashboard.render(f)).unwrap();
    terminal.backend().buffer().clone()
}

fn text(buffer: &Buffer) -> String {
    let area = buffer.area;
    let mut out = String::new();
    for y in 0..area.height {
        for x in 0..area.width {
            out.push_str(buffer[(x, y)].symbol());
        }
        out.push('\n');
    }
    out
}

/// The foreground colour of the first cell of `needle` on screen.
fn colour_of(buffer: &Buffer, needle: &str) -> Color {
    let area = buffer.area;
    for y in 0..area.height {
        let row: Vec<String> = (0..area.width)
            .map(|x| buffer[(x, y)].symbol().to_owned())
            .collect();
        let line = row.concat();
        if let Some(byte) = line.find(needle) {
            // Symbols are one cell each: count the characters before the match.
            let x = line[..byte].chars().count() as u16;
            return buffer[(x, y)].fg;
        }
    }
    panic!("{needle:?} not on screen:\n{}", text(buffer));
}

#[test]
fn tiles_show_the_last_seconds_numbers() {
    let mut dash = dashboard();
    dash.apply_progress(progress(12, 10));
    dash.apply_frame(&frame(12, 250.0, (5, 250), 120.0));
    let screen = text(&render(&dash));
    for expected in [
        "script.py",
        "10/20",
        "250",
        "2.00%",
        "120ms",
        "1,234",
        "00:12",
    ] {
        assert!(screen.contains(expected), "{expected:?} missing:\n{screen}");
    }
}

#[test]
fn thresholds_and_checks_are_coloured_by_outcome() {
    let theme = Theme::new(ThemeName::Brand, ColorDepth::TrueColor);
    let mut dash = dashboard();
    dash.apply_frame(&frame(1, 10.0, (0, 10), 5.0));
    let buffer = render(&dash);
    let screen = text(&buffer);
    assert!(screen.contains("p(95)<500"), "{screen}");
    assert_eq!(colour_of(&buffer, "✓ http_req_duration"), theme.ok);
    assert_eq!(colour_of(&buffer, "✗ http_req_failed"), theme.bad);
    assert!(
        screen.contains("status is 200") && screen.contains("99.00%"),
        "{screen}"
    );
    assert!(screen.contains("50.00%"), "{screen}");
}

#[test]
fn charts_keep_a_history_of_every_second() {
    let mut dash = dashboard();
    for s in 1..=5 {
        dash.apply_progress(progress(s, s as u32));
        dash.apply_frame(&frame(s, 100.0 * s as f64, (0, 1), 10.0 * s as f64));
    }
    assert_eq!(dash.history().len(), 5);
    let last = dash.history().back().unwrap();
    assert_eq!((last.vus, last.rps, last.p95), (5.0, 500.0, 50.0));
    let screen = text(&render(&dash));
    assert!(
        screen.contains("Requests/s") && screen.contains("Response time"),
        "{screen}"
    );
}

#[test]
fn errors_are_logged_and_pauses_shown() {
    let mut dash = dashboard();
    dash.log_event(RunEvent::RequestError(
        "could not connect to 10.0.0.1:80".into(),
    ));
    dash.set_paused(true);
    let screen = text(&render(&dash));
    assert!(
        screen.contains("could not connect to 10.0.0.1:80"),
        "{screen}"
    );
    assert!(screen.contains("PAUSED"), "{screen}");
}

#[test]
fn keys_control_the_run() {
    let key = |code| KeyEvent::new(code, KeyModifiers::NONE);
    assert_eq!(key_action(key(KeyCode::Char('q'))), Some(Action::Stop));
    assert_eq!(
        key_action(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
        Some(Action::Stop)
    );
    assert_eq!(
        key_action(key(KeyCode::Char('p'))),
        Some(Action::TogglePause)
    );
    assert_eq!(key_action(key(KeyCode::Char('+'))), Some(Action::Scale(1)));
    assert_eq!(key_action(key(KeyCode::Char('-'))), Some(Action::Scale(-1)));
    assert_eq!(key_action(key(KeyCode::Char('x'))), None);
}

#[test]
fn colours_follow_the_terminal() {
    assert_eq!(
        ColorDepth::detect(None, Some("truecolor")),
        ColorDepth::TrueColor
    );
    assert_eq!(
        ColorDepth::detect(None, Some("24bit")),
        ColorDepth::TrueColor
    );
    assert_eq!(ColorDepth::detect(None, None), ColorDepth::Indexed);
    assert_eq!(
        ColorDepth::detect(Some("1"), Some("truecolor")),
        ColorDepth::None
    );
    let brand = Theme::new(ThemeName::Brand, ColorDepth::TrueColor);
    assert_eq!(
        brand.accent,
        Color::Rgb(0xD1, 0x9E, 0x4B),
        "the logo's gold"
    );
    assert_eq!(
        brand.primary,
        Color::Rgb(0x0B, 0x6B, 0xE7),
        "the logo's blue"
    );
    assert!(matches!(
        Theme::new(ThemeName::Brand, ColorDepth::Indexed).accent,
        Color::Indexed(_)
    ));
    let neon = Theme::new(ThemeName::Neon, ColorDepth::TrueColor);
    assert_ne!(neon.accent, brand.accent);
    let plain = Theme::new(ThemeName::Neon, ColorDepth::None);
    assert_eq!(
        (plain.accent, plain.ok, plain.bad),
        (Color::Reset, Color::Reset, Color::Reset)
    );
    assert_eq!("neon".parse::<ThemeName>(), Ok(ThemeName::Neon));
    assert!("pink".parse::<ThemeName>().is_err());
}
