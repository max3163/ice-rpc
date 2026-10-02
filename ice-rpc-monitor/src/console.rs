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

/// Marker line that separates the metrics block from the recent messages.
///
/// Distinct from the metrics rules (a run of `-`), so the live view can split a
/// frame into its two parts and reserve room for the messages.
const MESSAGES_MARKER: &str = "---- recent messages ----";

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
    /// Terminal width in columns, from `COLUMNS`. A message longer than the
    /// screen wraps onto extra lines and pushes the list off the frame, so each
    /// message is truncated to this width.
    columns: usize,
}

impl LiveConsole {
    /// Enables the live mode when `enabled` **and** stdout is a terminal.
    pub fn new(enabled: bool) -> Self {
        let active = enabled && std::io::stdout().is_terminal();
        let console = Self {
            active,
            lines: terminal_lines(),
            columns: terminal_columns(),
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
            let body = fit_frame(frame, self.lines, self.columns);
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

/// Width of the terminal in columns.
///
/// `COLUMNS` is frequently not exported — notably on Windows — so the console is
/// asked **directly** when stdout is a terminal; `COLUMNS` is only a fallback.
/// When neither is available the width is treated as unbounded, so a message is
/// never truncated on a guess.
fn terminal_columns() -> usize {
    if std::io::stdout().is_terminal() {
        if let Some(columns) = console_columns() {
            return columns;
        }
    }
    std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|columns| *columns >= 20)
        .unwrap_or(usize::MAX)
}

/// Asks the operating system for the console width, in columns.
#[cfg(windows)]
fn console_columns() -> Option<usize> {
    use std::os::windows::io::AsRawHandle;

    #[repr(C)]
    #[derive(Default)]
    struct Coord {
        x: i16,
        y: i16,
    }
    #[repr(C)]
    #[derive(Default)]
    struct SmallRect {
        left: i16,
        top: i16,
        right: i16,
        bottom: i16,
    }
    #[repr(C)]
    #[derive(Default)]
    struct ScreenBufferInfo {
        size: Coord,
        cursor: Coord,
        attributes: u16,
        window: SmallRect,
        maximum_window_size: Coord,
    }

    extern "system" {
        fn GetConsoleScreenBufferInfo(
            console: *mut core::ffi::c_void,
            info: *mut ScreenBufferInfo,
        ) -> i32;
    }

    let handle = std::io::stdout().as_raw_handle();
    let mut info = ScreenBufferInfo::default();
    // SAFETY: `handle` is the process stdout handle (a valid console handle
    // whenever stdout is a terminal) and `info` is a valid, correctly laid-out
    // `CONSOLE_SCREEN_BUFFER_INFO` for the duration of the call.
    if unsafe { GetConsoleScreenBufferInfo(handle.cast(), &mut info) } == 0 {
        return None;
    }
    // The *window* is the visible area; the buffer can be wider (scrollback).
    let columns = i32::from(info.window.right) - i32::from(info.window.left) + 1;
    (columns > 0).then_some(columns as usize)
}

/// Asks the operating system for the console width, in columns.
#[cfg(target_os = "linux")]
fn console_columns() -> Option<usize> {
    #[repr(C)]
    #[derive(Default)]
    struct WinSize {
        rows: u16,
        columns: u16,
        x_pixels: u16,
        y_pixels: u16,
    }

    extern "C" {
        fn ioctl(fd: i32, request: u64, ...) -> i32;
    }
    // TIOCGWINSZ on Linux.
    const TIOCGWINSZ: u64 = 0x5413;

    let mut size = WinSize::default();
    // SAFETY: fd 1 is stdout; `TIOCGWINSZ` writes a `winsize` into the
    // correctly laid-out `WinSize` `size` points to, and reads nothing else.
    let ok = unsafe { ioctl(1, TIOCGWINSZ, &mut size) };
    (ok == 0 && size.columns > 0).then_some(size.columns as usize)
}

/// No portable query: fall back to `COLUMNS` (then to no truncation).
#[cfg(not(any(windows, target_os = "linux")))]
fn console_columns() -> Option<usize> {
    None
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

/// Truncates a block keeping **both** its head and its tail.
///
/// The metrics block ends with the network/node inventory, which must stay
/// visible; a head-only clamp would cut exactly that. So the middle is dropped
/// instead, with a notice, and the top counters and the bottom inventory both
/// survive.
fn clamp_middle(text: &str, max_lines: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() <= max_lines {
        return text.to_owned();
    }
    let kept = max_lines.saturating_sub(1); // one line for the notice
    let head = kept / 2;
    let tail = kept - head;
    let mut out = String::with_capacity(text.len());
    for line in &lines[..head] {
        out.push_str(line);
        out.push('\n');
    }
    out.push_str(&format!(
        "... ({} line(s) hidden, enlarge the terminal)\n",
        lines.len() - head - tail
    ));
    for line in &lines[lines.len() - tail..] {
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// Fits a whole frame to the screen.
///
/// The metrics block is the primary view, so it is kept whole and the messages
/// take the leftover lines; only when the metrics themselves overflow the screen
/// are they trimmed (head and tail kept, so the node inventory survives). Each
/// message is truncated to the terminal width, so a long decoded payload stays
/// on one line instead of wrapping and pushing the rest away.
fn fit_frame(frame: &str, max_lines: usize, max_width: usize) -> String {
    let Some((metrics, messages)) = frame.split_once(MESSAGES_MARKER) else {
        return clamp_frame(frame, max_lines);
    };
    let messages: Vec<&str> = messages.lines().filter(|line| !line.is_empty()).collect();
    if messages.is_empty() {
        return clamp_frame(metrics, max_lines);
    }

    // The metrics are the primary view: keep them whole and give the messages the
    // leftover lines. Only when the metrics themselves overflow the screen are
    // they trimmed — head and tail kept — to leave a share for the messages.
    let metrics_lines = metrics.lines().count();
    let reserved = if metrics_lines + 2 <= max_lines {
        0
    } else {
        (max_lines / 3).max(1)
    };
    let metrics_budget = max_lines.saturating_sub(reserved + 1).max(1);
    let mut out = clamp_middle(metrics, metrics_budget);
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(MESSAGES_MARKER);
    out.push('\n');

    // Show the most recent messages that still fit, oldest first.
    let room = max_lines.saturating_sub(out.lines().count());
    for line in messages.iter().rev().take(room).rev() {
        out.push_str(&truncate_width(line, max_width));
        out.push('\n');
    }
    out
}

/// Truncates a line to `max_width` columns, marking the cut with an ellipsis.
///
/// An "unbounded" width (`usize::MAX`, the unknown-terminal fallback) disables
/// the truncation entirely.
fn truncate_width(line: &str, max_width: usize) -> String {
    if max_width == usize::MAX || line.chars().count() <= max_width {
        return line.to_owned();
    }
    let keep = max_width.saturating_sub(1);
    let mut out: String = line.chars().take(keep).collect();
    out.push('…');
    out
}

/// Builds one frame: the metrics block, then the most recent messages.
pub fn frame(metrics: &Metrics, recent: Option<&RecentBuffer>) -> String {
    let mut frame = metrics.render_console();
    if let Some(recent) = recent {
        if let Ok(lines) = recent.lock() {
            if !lines.is_empty() {
                frame.push_str(MESSAGES_MARKER);
                frame.push('\n');
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
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Serialises the tests that read or write the process-global `LINES`
    /// variable: the harness runs the tests in threads, and `std::env` is shared.
    fn env_lock() -> &'static Mutex<()> {
        static LOCK: Mutex<()> = Mutex::new(());
        &LOCK
    }

    #[test]
    fn a_frame_appends_the_recent_messages() {
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
    fn a_frame_without_messages_holds_no_message_section() {
        let metrics = Metrics::new();
        assert!(!frame(&metrics, None).contains("recent messages"));

        // An empty buffer is as good as no buffer at all.
        let empty: RecentBuffer = Arc::new(Mutex::new(VecDeque::new()));
        assert!(!frame(&metrics, Some(&empty)).contains("recent messages"));
    }

    #[test]
    fn a_disabled_console_never_switches_screen() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        // Not a terminal in the test harness: the live mode must stay off.
        let console = LiveConsole::new(false);
        assert!(!console.is_active());
    }

    #[test]
    fn the_live_mode_needs_a_terminal_even_when_requested() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        // Requested explicitly, but the test harness pipes stdout: it stays off,
        // so the frames are appended instead of redrawn in place.
        let console = LiveConsole::new(true);
        assert!(!console.is_active());
    }

    #[test]
    fn an_inactive_console_appends_and_leaves_cleanly() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let mut console = LiveConsole::new(false);
        console.draw("plain frame\n");
        console.leave();
        // `leave` is idempotent: the `Drop` guard calls it again.
        console.leave();
        assert!(!console.is_active());
    }

    #[test]
    fn the_terminal_height_comes_from_the_environment() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let previous = std::env::var("LINES").ok();

        std::env::set_var("LINES", "42");
        assert_eq!(terminal_lines(), 42);
        // A frame taller than the screen is worse than no frame at all, so an
        // unusable value falls back to the conservative default.
        std::env::set_var("LINES", "4");
        assert_eq!(terminal_lines(), 24);
        std::env::set_var("LINES", "not-a-number");
        assert_eq!(terminal_lines(), 24);

        match previous {
            Some(value) => std::env::set_var("LINES", value),
            None => std::env::remove_var("LINES"),
        }
    }

    #[test]
    fn a_short_frame_is_left_untouched() {
        let frame = "a\nb\nc\n";
        assert_eq!(clamp_frame(frame, 10), frame);
    }

    #[test]
    fn a_frame_exactly_the_screen_height_is_not_truncated() {
        let frame: String = (0..10).map(|index| format!("line {index}\n")).collect();
        assert_eq!(clamp_frame(&frame, 10), frame);
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

    #[test]
    fn a_one_line_screen_keeps_only_the_summary() {
        let frame: String = (0..5).map(|index| format!("line {index}\n")).collect();
        let clamped = clamp_frame(&frame, 1);
        assert!(clamped.contains("more line(s)"), "{clamped}");
        assert!(!clamped.contains("line 0"), "{clamped}");
    }

    #[test]
    fn the_terminal_width_comes_from_the_environment() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let previous = std::env::var("COLUMNS").ok();

        std::env::set_var("COLUMNS", "100");
        assert_eq!(terminal_columns(), 100);
        // Too narrow to be trusted, and no console to ask (the test harness pipes
        // stdout): the width is treated as unbounded, so nothing is truncated.
        std::env::set_var("COLUMNS", "5");
        assert_eq!(terminal_columns(), usize::MAX);

        match previous {
            Some(value) => std::env::set_var("COLUMNS", value),
            None => std::env::remove_var("COLUMNS"),
        }
    }

    #[test]
    fn the_metrics_tail_is_kept_when_messages_are_shown() {
        // A tall metrics block whose distinctive line (the node inventory) is at
        // the very end, then one message.
        let mut metrics: String = (0..40).map(|index| format!("metric {index}\n")).collect();
        metrics.push_str("network : nodes=3 (alive 3, dead 0)  services=5\n");
        let mut frame = metrics;
        frame.push_str(MESSAGES_MARKER);
        frame.push('\n');
        frame.push_str("[msg] hello\n");

        let fitted = fit_frame(&frame, 12, 80);
        assert!(fitted.lines().count() <= 12, "{fitted}");
        // The metrics tail survives: a head-only clamp used to cut it.
        assert!(
            fitted.contains("network : nodes=3 (alive 3, dead 0)  services=5"),
            "{fitted}"
        );
        assert!(fitted.contains("[msg] hello"), "{fitted}");
        assert!(fitted.contains("line(s) hidden"), "{fitted}");
    }

    #[test]
    fn the_messages_keep_a_share_of_a_tall_metrics_block() {
        // A metrics block far taller than the screen, then two messages.
        let mut frame: String = (0..40).map(|index| format!("metric {index}\n")).collect();
        frame.push_str(MESSAGES_MARKER);
        frame.push('\n');
        frame.push_str("[msg] hello\n");
        frame.push_str("[msg] world\n");

        let fitted = fit_frame(&frame, 10, 80);
        assert!(fitted.lines().count() <= 10, "{fitted}");
        // The messages are still shown, even though the metrics overflow.
        assert!(fitted.contains("[msg] hello"), "{fitted}");
        assert!(fitted.contains("[msg] world"), "{fitted}");
    }

    #[test]
    fn a_whole_metrics_block_is_kept_when_it_fits() {
        // Room for the metrics *and* the messages: nothing is hidden.
        let metrics: String = (0..20).map(|index| format!("metric {index}\n")).collect();
        let mut frame = metrics;
        frame.push_str(MESSAGES_MARKER);
        frame.push('\n');
        frame.push_str("[msg] hi\n");

        let fitted = fit_frame(&frame, 30, 80);
        assert!(!fitted.contains("hidden"), "{fitted}");
        assert!(fitted.contains("metric 19"), "{fitted}");
        assert!(fitted.contains("[msg] hi"), "{fitted}");
    }

    #[test]
    fn a_long_message_is_truncated_to_the_terminal_width() {
        let long = "x".repeat(200);
        let truncated = truncate_width(&long, 20);
        assert_eq!(truncated.chars().count(), 20);
        assert!(truncated.ends_with('…'));
        // A message shorter than the screen is left untouched.
        assert_eq!(truncate_width("short", 20), "short");
    }

    #[test]
    fn a_frame_without_messages_is_still_clamped() {
        let frame: String = (0..40).map(|index| format!("line {index}\n")).collect();
        let fitted = fit_frame(&frame, 10, 80);
        assert_eq!(fitted.lines().count(), 10);
        assert!(fitted.contains("more line(s)"), "{fitted}");
    }
}
