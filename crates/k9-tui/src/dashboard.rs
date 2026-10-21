//! The dashboard's state (fed by the engine's progress, frames and events)
//! and how it's drawn.

use std::collections::VecDeque;
use std::time::Duration;

use k9_core::engine::{LiveFrame, Progress, RunEvent, ScriptError};
use k9_metrics::Stats;
use k9_output::format;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Axis, Block, BorderType, Chart, Dataset, Gauge, GraphType, Paragraph, Sparkline,
};

use crate::theme::Theme;

/// Seconds of history the charts keep.
const HISTORY: usize = 3600;
/// Log lines kept.
const LOG: usize = 200;

/// What the run is about, from its header.
#[derive(Debug, Clone)]
pub struct Header {
    pub script: String,
    pub max_vus: u32,
    /// Longest possible run, including graceful stops.
    pub max_duration: Duration,
}

/// One second of the run, for the charts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub secs: f64,
    pub vus: f64,
    /// Requests that second.
    pub rps: f64,
    /// Failed requests that second.
    pub failed: f64,
    /// `http_req_duration` that second, in ms.
    pub p95: f64,
    pub p99: f64,
    pub avg: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Level {
    Info,
    Warn,
    Error,
}

pub struct Dashboard {
    header: Header,
    theme: Theme,
    progress: Progress,
    frame: LiveFrame,
    history: VecDeque<Point>,
    log: VecDeque<(Level, String)>,
    paused: bool,
    stopping: bool,
}

impl Dashboard {
    pub fn new(header: Header, theme: Theme) -> Dashboard {
        Dashboard {
            header,
            theme,
            progress: Progress::default(),
            frame: LiveFrame::default(),
            history: VecDeque::new(),
            log: VecDeque::new(),
            paused: false,
            stopping: false,
        }
    }

    pub fn apply_progress(&mut self, progress: Progress) {
        self.progress = progress;
    }

    /// A new second: its numbers become the tiles and a point on the charts.
    pub fn apply_frame(&mut self, frame: &LiveFrame) {
        let (p95, p99, avg) = match frame.second("http_req_duration") {
            Some(Stats::Trend { p95, p99, avg, .. }) => (p95, p99, avg),
            _ => (0.0, 0.0, 0.0),
        };
        let failed = match frame.second("http_req_failed") {
            Some(Stats::Rate { trues, .. }) => trues as f64,
            _ => 0.0,
        };
        if self.history.len() == HISTORY {
            self.history.pop_front();
        }
        self.history.push_back(Point {
            secs: frame.elapsed.as_secs_f64(),
            vus: f64::from(self.progress.active_vus),
            rps: counter(frame, "http_reqs"),
            failed,
            p95,
            p99,
            avg,
        });
        self.frame = frame.clone();
    }

    pub fn history(&self) -> &VecDeque<Point> {
        &self.history
    }

    /// A script exception or request error, as the plain output prints it.
    pub fn log_event(&mut self, event: RunEvent) {
        match event {
            RunEvent::ScriptError(ScriptError { message, details }) => {
                // Tracebacks are long: the log shows their last line.
                let text = details
                    .as_deref()
                    .and_then(|d| d.lines().rev().find(|l| !l.trim().is_empty()))
                    .map_or(message, str::to_owned);
                self.log(Level::Error, format!("ERROR {text}"));
            }
            RunEvent::RequestError(message) => {
                self.log(Level::Warn, format!("WARN request failed: {message}"));
            }
        }
    }

    /// A line of feedback, e.g. the answer to a key press.
    pub fn note(&mut self, text: impl Into<String>) {
        self.log(Level::Info, text.into());
    }

    pub fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }

    pub fn is_paused(&self) -> bool {
        self.paused
    }

    pub fn set_stopping(&mut self) {
        self.stopping = true;
    }

    pub fn is_stopping(&self) -> bool {
        self.stopping
    }

    /// Active VUs, as last reported.
    pub fn vus(&self) -> u32 {
        self.progress.active_vus
    }

    fn log(&mut self, level: Level, text: String) {
        if self.log.len() == LOG {
            self.log.pop_front();
        }
        self.log.push_back((level, text));
    }

    pub fn render(&self, f: &mut Frame<'_>) {
        let [top, tiles, charts, vus, lists, log, keys] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(3),
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Length(1),
        ])
        .areas(f.area());
        self.render_top(f, top);
        self.render_tiles(f, tiles);
        let [left, right] =
            Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
                .areas(charts);
        self.render_requests(f, left);
        self.render_times(f, right);
        self.render_vus(f, vus);
        let [checks, thresholds] =
            Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
                .areas(lists);
        self.render_checks(f, checks);
        self.render_thresholds(f, thresholds);
        self.render_log(f, log);
        self.render_keys(f, keys);
    }

    fn block<'a>(&self, title: &'a str) -> Block<'a> {
        Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(Style::new().fg(self.theme.primary))
            .title(Span::styled(
                format!(" {title} "),
                Style::new()
                    .fg(self.theme.accent)
                    .add_modifier(Modifier::BOLD),
            ))
    }

    fn render_top(&self, f: &mut Frame<'_>, area: Rect) {
        let t = &self.theme;
        let (state, colour) = if self.stopping {
            ("■ STOPPING", t.bad)
        } else if self.paused {
            ("‖ PAUSED", t.warn)
        } else {
            ("● RUNNING", t.ok)
        };
        let [text, bar] =
            Layout::horizontal([Constraint::Min(40), Constraint::Length(30)]).areas(area);
        let line = Line::from(vec![
            Span::styled(
                " K9 ",
                Style::new()
                    .fg(t.accent)
                    .add_modifier(Modifier::BOLD | Modifier::REVERSED),
            ),
            Span::styled(" ▸ ", Style::new().fg(t.muted)),
            Span::styled(
                self.header.script.clone(),
                Style::new().add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(state, Style::new().fg(colour).add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(
                format!(
                    "{} / {}",
                    clock(self.progress.elapsed),
                    clock(self.header.max_duration)
                ),
                Style::new().fg(t.primary),
            ),
        ]);
        f.render_widget(Paragraph::new(line), text);
        let total = self.header.max_duration.as_secs_f64();
        let ratio = if total > 0.0 {
            (self.progress.elapsed.as_secs_f64() / total).clamp(0.0, 1.0)
        } else {
            0.0
        };
        f.render_widget(
            Gauge::default()
                .gauge_style(Style::new().fg(t.accent))
                .ratio(ratio)
                .label(format!("{:.0}%", ratio * 100.0)),
            bar,
        );
    }

    fn render_tiles(&self, f: &mut Frame<'_>, area: Rect) {
        let t = &self.theme;
        let failed = match self.frame.second("http_req_failed") {
            Some(Stats::Rate { trues, total }) if total > 0 => Some(trues as f64 / total as f64),
            _ => None,
        };
        let p95 = match self.frame.second("http_req_duration") {
            Some(Stats::Trend { p95, .. }) => Some(p95),
            _ => None,
        };
        let total = |metric: &str| {
            self.frame
                .totals
                .iter()
                .find(|m| m.name == metric)
                .and_then(|m| m.sample.iter().find(|(k, _)| k == "count"))
                .map_or(0.0, |(_, v)| *v)
        };
        let tiles = [
            (
                "VUs",
                format!("{}/{}", self.progress.active_vus, self.header.max_vus),
                t.accent,
            ),
            (
                "Requests/s",
                grouped(counter(&self.frame, "http_reqs")),
                t.primary,
            ),
            (
                "Failed",
                failed.map_or("-".to_owned(), format::percent),
                match failed {
                    Some(share) if share > 0.0 => t.bad,
                    _ => t.ok,
                },
            ),
            (
                "p95",
                p95.map_or("-".to_owned(), format::duration_ms),
                t.p95,
            ),
            (
                "Iterations",
                grouped(self.progress.iterations_complete as f64),
                t.avg,
            ),
            (
                "Data ↓/↑",
                format!(
                    "{} / {}",
                    format::bytes(total("data_received")),
                    format::bytes(total("data_sent"))
                ),
                t.warn,
            ),
        ];
        let areas = Layout::horizontal([Constraint::Ratio(1, tiles.len() as u32); 6]).split(area);
        for ((label, value, colour), area) in tiles.into_iter().zip(areas.iter()) {
            let block = Block::bordered()
                .border_type(BorderType::Rounded)
                .border_style(Style::new().fg(t.muted))
                .title(Span::styled(format!(" {label} "), Style::new().fg(t.muted)));
            f.render_widget(
                Paragraph::new(Span::styled(
                    value,
                    Style::new().fg(colour).add_modifier(Modifier::BOLD),
                ))
                .centered()
                .block(block),
                *area,
            );
        }
    }

    fn chart(&self, f: &mut Frame<'_>, area: Rect, title: &str, series: &[Series<'_>]) {
        let t = &self.theme;
        let points: Vec<Vec<(f64, f64)>> = series
            .iter()
            .map(|s| {
                self.history
                    .iter()
                    .map(|p| (p.secs, (s.value)(p)))
                    .collect()
            })
            .collect();
        let x_min = self.history.front().map_or(0.0, |p| p.secs);
        let x_max = self.history.back().map_or(1.0, |p| p.secs).max(x_min + 1.0);
        let y_max = points
            .iter()
            .flatten()
            .map(|(_, y)| *y)
            .fold(0.0, f64::max)
            .max(1.0)
            * 1.1;
        let datasets = series
            .iter()
            .zip(&points)
            .map(|(s, data)| {
                Dataset::default()
                    .name(s.name)
                    .marker(Marker::Braille)
                    .graph_type(GraphType::Line)
                    .style(Style::new().fg(s.colour))
                    .data(data)
            })
            .collect();
        let label = |v: f64| Span::styled(axis_label(v, title), Style::new().fg(t.muted));
        // The title names each series in its colour, in place of a legend
        // that would cover the lines.
        let mut heading = vec![Span::styled(
            format!(" {title} "),
            Style::new().fg(t.accent).add_modifier(Modifier::BOLD),
        )];
        for s in series {
            heading.push(Span::styled(
                format!("━ {} ", s.name),
                Style::new().fg(s.colour),
            ));
        }
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(Style::new().fg(t.primary))
            .title(Line::from(heading));
        let chart = Chart::new(datasets)
            .block(block)
            .legend_position(None)
            .x_axis(
                Axis::default()
                    .style(Style::new().fg(t.muted))
                    .bounds([x_min, x_max])
                    .labels([
                        Span::styled(clock(secs(x_min)), Style::new().fg(t.muted)),
                        Span::styled(clock(secs(x_max)), Style::new().fg(t.muted)),
                    ]),
            )
            .y_axis(
                Axis::default()
                    .style(Style::new().fg(t.muted))
                    .bounds([0.0, y_max])
                    .labels([label(0.0), label(y_max / 2.0), label(y_max)]),
            );
        f.render_widget(chart, area);
    }

    fn render_requests(&self, f: &mut Frame<'_>, area: Rect) {
        let t = &self.theme;
        self.chart(
            f,
            area,
            "Requests/s",
            &[
                Series {
                    name: "requests",
                    colour: t.primary,
                    value: |p| p.rps,
                },
                Series {
                    name: "failed",
                    colour: t.bad,
                    value: |p| p.failed,
                },
            ],
        );
    }

    fn render_times(&self, f: &mut Frame<'_>, area: Rect) {
        let t = &self.theme;
        self.chart(
            f,
            area,
            "Response time",
            &[
                Series {
                    name: "p99",
                    colour: t.p99,
                    value: |p| p.p99,
                },
                Series {
                    name: "p95",
                    colour: t.p95,
                    value: |p| p.p95,
                },
                Series {
                    name: "avg",
                    colour: t.avg,
                    value: |p| p.avg,
                },
            ],
        );
    }

    fn render_vus(&self, f: &mut Frame<'_>, area: Rect) {
        let width = area.width.saturating_sub(2) as usize;
        let data: Vec<u64> = self
            .history
            .iter()
            .rev()
            .take(width)
            .rev()
            .map(|p| p.vus as u64)
            .collect();
        f.render_widget(
            Sparkline::default()
                .block(self.block("VUs"))
                .style(Style::new().fg(self.theme.accent))
                .max(u64::from(self.header.max_vus.max(1)))
                .data(&data),
            area,
        );
    }

    fn render_checks(&self, f: &mut Frame<'_>, area: Rect) {
        let t = &self.theme;
        let bar_width = 12usize;
        let lines: Vec<Line<'_>> = self
            .frame
            .checks
            .iter()
            .map(|c| {
                let total = c.passes + c.fails;
                let share = if total > 0 {
                    c.passes as f64 / total as f64
                } else {
                    0.0
                };
                let (mark, colour) = if c.fails == 0 {
                    ("✓", t.ok)
                } else {
                    ("✗", t.bad)
                };
                let filled = (share * bar_width as f64).round() as usize;
                Line::from(vec![
                    Span::styled(format!("{mark} {}", c.name), Style::new().fg(colour)),
                    Span::raw("  "),
                    Span::styled("█".repeat(filled), Style::new().fg(t.ok)),
                    Span::styled("█".repeat(bar_width - filled), Style::new().fg(t.bad)),
                    Span::styled(
                        format!(" {}", format::percent(share)),
                        Style::new().add_modifier(Modifier::BOLD),
                    ),
                ])
            })
            .collect();
        let body = if lines.is_empty() {
            Paragraph::new(Span::styled("no checks yet", Style::new().fg(t.muted)))
        } else {
            Paragraph::new(lines)
        };
        f.render_widget(body.block(self.block("Checks")), area);
    }

    fn render_thresholds(&self, f: &mut Frame<'_>, area: Rect) {
        let t = &self.theme;
        let lines: Vec<Line<'_>> = self
            .frame
            .thresholds
            .iter()
            .map(|o| {
                let (mark, colour) = if o.passed {
                    ("✓", t.ok)
                } else {
                    ("✗", t.bad)
                };
                Line::from(vec![
                    Span::styled(
                        format!("{mark} {}", o.selector),
                        Style::new().fg(colour).add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(format!("  {}", o.expression)),
                    Span::styled(
                        format!(
                            "  now {}",
                            format::number((o.observed * 100.0).round() / 100.0)
                        ),
                        Style::new().fg(t.muted),
                    ),
                ])
            })
            .collect();
        let body = if lines.is_empty() {
            Paragraph::new(Span::styled("no thresholds", Style::new().fg(t.muted)))
        } else {
            Paragraph::new(lines)
        };
        f.render_widget(body.block(self.block("Thresholds")), area);
    }

    fn render_log(&self, f: &mut Frame<'_>, area: Rect) {
        let t = &self.theme;
        let height = area.height.saturating_sub(2) as usize;
        let lines: Vec<Line<'_>> = self
            .log
            .iter()
            .rev()
            .take(height)
            .rev()
            .map(|(level, text)| {
                let colour = match level {
                    Level::Info => t.primary,
                    Level::Warn => t.warn,
                    Level::Error => t.bad,
                };
                Line::styled(text.as_str(), Style::new().fg(colour))
            })
            .collect();
        f.render_widget(Paragraph::new(lines).block(self.block("Log")), area);
    }

    fn render_keys(&self, f: &mut Frame<'_>, area: Rect) {
        let t = &self.theme;
        let key = |k: &'static str| {
            Span::styled(
                k,
                Style::new()
                    .fg(t.accent)
                    .add_modifier(Modifier::BOLD | Modifier::REVERSED),
            )
        };
        let label = |l: &'static str| Span::styled(l, Style::new().fg(t.muted));
        let pause = if self.paused { " resume  " } else { " pause  " };
        f.render_widget(
            Paragraph::new(Line::from(vec![
                key(" p "),
                label(pause),
                key(" + "),
                key(" - "),
                label(" VUs  "),
                key(" q "),
                label(" stop (twice: exit now)"),
            ])),
            area,
        );
    }
}

struct Series<'a> {
    name: &'a str,
    colour: ratatui::style::Color,
    value: fn(&Point) -> f64,
}

fn counter(frame: &LiveFrame, metric: &str) -> f64 {
    match frame.second(metric) {
        Some(Stats::Counter { sum }) => sum,
        _ => 0.0,
    }
}

/// An axis label: whole ms for response times, plain numbers otherwise.
fn axis_label(value: f64, title: &str) -> String {
    if title == "Response time" {
        format::duration_ms(value.round())
    } else {
        grouped(value)
    }
}

fn secs(s: f64) -> Duration {
    Duration::from_secs_f64(s.max(0.0))
}

/// `mm:ss`, or `h:mm:ss` from an hour on.
fn clock(d: Duration) -> String {
    let s = d.as_secs();
    if s >= 3600 {
        format!("{}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
    } else {
        format!("{:02}:{:02}", s / 60, s % 60)
    }
}

/// A whole number with thousands separators: `1,234`.
fn grouped(value: f64) -> String {
    let digits = format!("{:.0}", value.max(0.0));
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_and_clocks() {
        assert_eq!(grouped(0.0), "0");
        assert_eq!(grouped(999.0), "999");
        assert_eq!(grouped(1234.4), "1,234");
        assert_eq!(grouped(1234567.0), "1,234,567");
        assert_eq!(clock(Duration::from_secs(72)), "01:12");
        assert_eq!(clock(Duration::from_secs(3723)), "1:02:03");
    }
}
