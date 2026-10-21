//! Colours: the logo's gold and blue (`brand`) or a louder `neon`, in 24-bit
//! colour when the terminal says it has it, the nearest of 256 otherwise,
//! and none with `NO_COLOR`.

use std::str::FromStr;

use ratatui::style::Color;

/// How many colours the terminal shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorDepth {
    TrueColor,
    /// The 256-colour palette.
    Indexed,
    /// `NO_COLOR` is set: the terminal's own colours only.
    None,
}

impl ColorDepth {
    /// From `NO_COLOR` and `COLORTERM` (<https://no-color.org>).
    pub fn detect(no_color: Option<&str>, colorterm: Option<&str>) -> ColorDepth {
        if no_color.is_some_and(|v| !v.is_empty()) {
            return ColorDepth::None;
        }
        match colorterm {
            Some("truecolor" | "24bit") => ColorDepth::TrueColor,
            _ => ColorDepth::Indexed,
        }
    }

    /// From this process's environment.
    pub fn from_env() -> ColorDepth {
        let no_color = std::env::var("NO_COLOR").ok();
        let colorterm = std::env::var("COLORTERM").ok();
        ColorDepth::detect(no_color.as_deref(), colorterm.as_deref())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ThemeName {
    /// The logo's gold (`#D19E4B`) and blue (`#0B6BE7`) with vivid status colours.
    #[default]
    Brand,
    /// Hot pink, cyan and acid green.
    Neon,
}

impl FromStr for ThemeName {
    type Err = String;

    fn from_str(s: &str) -> Result<ThemeName, String> {
        match s {
            "brand" => Ok(ThemeName::Brand),
            "neon" => Ok(ThemeName::Neon),
            other => Err(format!("unknown theme {other:?} (expected brand or neon)")),
        }
    }
}

/// Every colour the dashboard uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Theme {
    /// Titles, the VUs series.
    pub accent: Color,
    /// Borders, the requests series.
    pub primary: Color,
    pub ok: Color,
    pub bad: Color,
    pub warn: Color,
    pub muted: Color,
    /// Response-time series: p95, p99, avg.
    pub p95: Color,
    pub p99: Color,
    pub avg: Color,
}

impl Theme {
    pub fn new(name: ThemeName, depth: ColorDepth) -> Theme {
        let rgb = |hex: u32| colour(hex, depth);
        match name {
            ThemeName::Brand => Theme {
                accent: rgb(0xD19E4B),
                primary: rgb(0x0B6BE7),
                ok: rgb(0x3DDC84),
                bad: rgb(0xFF4D5E),
                warn: rgb(0xFFB020),
                muted: rgb(0x8A8F98),
                p95: rgb(0xE040FB),
                p99: rgb(0xFF6E40),
                avg: rgb(0x18FFFF),
            },
            ThemeName::Neon => Theme {
                accent: rgb(0xFF2BD6),
                primary: rgb(0x00E5FF),
                ok: rgb(0x39FF14),
                bad: rgb(0xFF3131),
                warn: rgb(0xFFE600),
                muted: rgb(0x9A9AB0),
                p95: rgb(0xB026FF),
                p99: rgb(0xFF8C00),
                avg: rgb(0x00FFA3),
            },
        }
    }
}

fn colour(hex: u32, depth: ColorDepth) -> Color {
    let (r, g, b) = ((hex >> 16) as u8, (hex >> 8) as u8, hex as u8);
    match depth {
        ColorDepth::TrueColor => Color::Rgb(r, g, b),
        ColorDepth::Indexed => Color::Indexed(nearest_256(r, g, b)),
        ColorDepth::None => Color::Reset,
    }
}

/// The nearest colour of xterm's 6×6×6 cube (indexes 16–231).
fn nearest_256(r: u8, g: u8, b: u8) -> u8 {
    const LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];
    let step = |v: u8| {
        (0..6u8)
            .min_by_key(|&i| (i16::from(LEVELS[i as usize]) - i16::from(v)).abs())
            .expect("six levels")
    };
    16 + 36 * step(r) + 6 * step(g) + step(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cube_maps_primaries_and_greys() {
        assert_eq!(nearest_256(0, 0, 0), 16);
        assert_eq!(nearest_256(255, 255, 255), 231);
        assert_eq!(nearest_256(255, 0, 0), 196);
        // Brand gold #D19E4B: levels 215, 175, 95.
        assert_eq!(nearest_256(0xD1, 0x9E, 0x4B), 16 + 36 * 4 + 6 * 3 + 1);
    }
}
