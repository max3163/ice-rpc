//! `top`-like live rendering of the metrics on a terminal.
//!
//! The frame is redrawn **in place** using the alternate screen buffer, so
//! nothing scrolls and the terminal content is restored on exit. The mode is
//! opt-in (`--live`) and falls back to plain appending when stdout is not a
//! terminal (a pipe, a file, an IDE without a TTY), so a redirected run stays
//! machine-readable.
//!
//! Quitting is the usual `Ctrl+C`: the [`Drop`] implementation restores the
//! terminal even on an early return.

use std::io::{IsTerminal, Write};

use crate::metrics::Metrics;
use crate::traces::RecentBuffer;

/// Separator drawn above the recent messages.
const RULE: &str = "------------------------------------------------------------";

/// Drives a full-screen, scroll-free console view.
pub struct LiveConsole {
    active: bool,
    /// Maximum number of lines that fit on the terminal.
    ///
    /// Read from `LINES` (usually exported by the shell) and capped to a
    /// conservative default: a frame taller than the terminal makes it scroll,
    /// and the next `cursor home` then overwrites the wrong region, which is
    /// what garbles the display.
    lines: usize,
}

impl LiveConsole {
    /// Enables the live mode when `enabled` **and** stdout is a terminal.
    pub fn new(enabled: bool) -> Self {
        let active = enabled && std::io::stdout().is_terminal();
        let console = Self {
            active,
            lines: terminal_lines(),
        };
        if console.active {
            // Alternate screen + hidden cursor: the caller's scrollback is left
            // untouched and restored by `leave`.
            console.write_raw("\x1b[?1049h\x1b[?25l\x1b[2J\x1b[H");
        }
        console
    }

    /// Whether the live mode is active (a terminal was detected).
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Draws one frame: in live mode it replaces the previous one in place,
    /// otherwise it is simply appended.
    pub fn draw(&self, frame: &str) {
        if self.active {
            let body = clamp_frame(frame, self.lines);
            let mut out = String::with_capacity(body.len() + 16);
            out.push_str("\x1b[H\x1b[2J"); // cursor home + clear the whole screen
            out.push_str(&body);
            self.write_raw(&out);
        } else {
            self.write_raw(frame);
        }
    }

    /// Restores the terminal (idempotent). Also called on drop.
    pub fn leave(&mut self) {
        if self.active {
            self.active = false;
            self.write_raw("\x1b[?25h\x1b[?1049l");
        }
    }

    /// Writes raw bytes to stdout and flushes.
    fn write_raw(&self, text: &str) {
        let mut stdout = std::io::stdout();
        let _ = stdout.write_all(text.as_bytes());
        let _ = stdout.flush();
    }
}

impl Drop for LiveConsole {
    fn drop(&mut self) {
        self.leave();
    }
}

/// Number of lines of the terminal, from `LINES` when available.
fn terminal_lines() -> usize {
    std::env::var("LINES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|lines| *lines >= 8)
        .unwrap_or(24)
}

/// Truncates a frame so it always fits on one screen.
///
/// Keeping the frame shorter than the terminal is what prevents scrolling, and
/// therefore the garbled overlapping frames a `top`-like view must avoid.
fn clamp_frame(frame: &str, max_lines: usize) -> String {
    let total = frame.lines().count();
    if total <= max_lines {
        return frame.to_owned();
    }
    let keep = max_lines.saturating_sub(1);
    let mut out = String::with_capacity(frame.len());
    for line in frame.lines().take(keep) {
        out.push_str(line);
        out.push('\n');
    }
    out.push_str(&format!(
        "... ({} more line(s), enlarge the terminal)\n",
        total - keep
    ));
    out
}

/// Builds one frame: the metrics block, then the most recent messages.
pub fn frame(metrics: &Metrics, recent: Option<&RecentBuffer>) -> String {
    let mut frame = metrics.render_console();
    if let Some(recent) = recent {
        if let Ok(lines) = recent.lock() {
            if !lines.is_empty() {
                frame.push_str(RULE);
                frame.push_str("\n recent messages\n");
                for line in lines.iter() {
                    frame.push_str(line);
                    frame.push('\n');
                }
            }
        }
    }
    frame
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_appends_the_recent_messages() {
        use std::collections::VecDeque;
        use std::sync::{Arc, Mutex};

        let metrics = Metrics::new();
        let recent: RecentBuffer = Arc::new(Mutex::new(VecDeque::from([
            "[msg] cid=1 request=ping".to_owned(),
            "[msg] cid=2 response=pong".to_owned(),
        ])));

        let frame = frame(&metrics, Some(&recent));
        assert!(frame.contains("recent messages"));
        assert!(frame.contains("[msg] cid=1 request=ping"));
        assert!(frame.contains("[msg] cid=2 response=pong"));
    }

    #[test]
    fn a_disabled_console_never_switches_screen() {
        // Not a terminal in the test harness: the live mode must stay off.
        let console = LiveConsole::new(false);
        assert!(!console.is_active());
    }

    #[test]
    fn a_short_frame_is_left_untouched() {
        let frame = "a\nb\nc\n";
        assert_eq!(clamp_frame(frame, 10), frame);
    }

    #[test]
    fn a_tall_frame_is_truncated_to_the_screen() {
        let frame: String = (0..40).map(|index| format!("line {index}\n")).collect();
        let clamped = clamp_frame(&frame, 10);
        assert_eq!(clamped.lines().count(), 10);
        assert!(clamped.contains("more line(s)"), "{clamped}");
        // The tail of the original frame is gone, never overflowing the screen.
        assert!(!clamped.contains("line 39"));
    }
}
