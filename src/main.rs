mod grid;

use crate::grid::*;
use anyhow::Context;
use crossterm::*;
use ndarray::prelude::*;
use ndarray_csv::Array2Reader;
use std::cmp::min;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use tempfile::*;

/// A pager for tabular data
#[derive(StructOpt)]
struct Opts {
    path: Option<PathBuf>,
}

fn main() {
    let opts = Opts::from_args();
    match main_2(opts) {
        Ok(()) => (),
        Err(e) => {
            eprintln!("Error: {}", e);
            for e in e.chain() {
                eprintln!("{}", e);
            }
            std::process::exit(1);
        }
    }
}

fn main_2(opts: Opts) -> anyhow::Result<()> {
    let path = match opts.path {
        Some(path) => path,
        None => {
            let mut file = NamedTempFile::new().context("creating tempfile")?;
            let path = file.path().to_owned();
            std::thread::spawn(move || {
                let stdin = std::io::stdin();
                let stdin = BufReader::new(stdin.lock());
                // Try to push a whole line atomically - otherwise the main
                // thread may see a line with the wrong number of columns.
                for line in stdin.lines() {
                    let mut line = line.unwrap();
                    line.push('\n');
                    file.write_all(line.as_bytes())
                        .context("filling tempfile")
                        .unwrap();
                }
            });
            path
        }
    };

    let mut newlines = LineOffsets::new(&path).context("generating offsets")?;
    let n = newlines.len();
    const MIN_LINES: usize = 3;
    if n < MIN_LINES {
        eprintln!(
            "Insufficient lines (saw {} but need at least {}).  Waiting for more data...",
            n, MIN_LINES
        );
        while newlines.len() < MIN_LINES {
            newlines.update()?;
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    // Set up terminal
    terminal::enable_raw_mode().context("entering raw mode")?;
    stdout.queue(terminal::EnterAlternateScreen)?.flush()?;

    // Store the result so the cleanup happens even if there's an error
    let result = main_3(newlines, &path, &mut stdout);

    // Clean up terminal
    stdout.queue(terminal::LeaveAlternateScreen)?.flush()?;
    terminal::disable_raw_mode()?;
    result
}

fn take_range(file: &mut File, r: Range<u64>) -> std::io::Result<impl Read + '_> {
    file.seek(SeekFrom::Start(r.start))?;
    Ok(file.take(r.end - r.start))
}

struct LineOffsets {
    offset: u64,
    newlines: Vec<u64>,
    file: BufReader<File>,
}
impl LineOffsets {
    fn new(path: &Path) -> anyhow::Result<LineOffsets> {
        let mut ret = LineOffsets {
            offset: 0,
            file: BufReader::new(File::open(path)?),
            newlines: vec![],
        };
        ret.update()?;
        Ok(ret)
    }
    fn update(&mut self) -> anyhow::Result<()> {
        loop {
            let buf = self.file.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            if let Some(x) = memchr::memchr(b'\n', buf) {
                self.newlines.push(self.offset + x as u64);
                self.offset += x as u64 + 1;
                self.file.consume(x + 1);
            } else {
                let x = buf.len();
                self.offset += x as u64;
                self.file.consume(x);
            }
        }
        Ok(())
    }
    /// Gives a byte-range which doesn't include the newline
    fn line2range(&self, line: usize) -> Range<u64> {
        let lhs = if line == 0 {
            0
        } else {
            self.newlines[line - 1] as u64 + 1
        };
        let rhs = self.newlines[line] as u64;
        lhs..rhs
    }
    fn len(&self) -> usize {
        self.newlines.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let mut f = NamedTempFile::new().unwrap();
        let s = b"foo,bar\n1,2\n3,4\n";
        f.write_all(s).unwrap();
        let lines = LineOffsets::new(f.path()).unwrap();
        assert_eq!(lines.len(), 3);
        // line2range never includes the newline char, hence the non-contiguous
        // ranges
        assert_eq!(lines.line2range(0), 0..7);
        assert_eq!(lines.line2range(1), 8..11);
        assert_eq!(lines.line2range(2), 12..15);
        assert_eq!(s.len(), 16);
    }
}

fn main_3(newlines: LineOffsets, path: &Path, stdout: &mut impl Write) -> anyhow::Result<()> {
    let mut file = File::open(path)?;
    let hdrs = csv::Reader::from_reader(&mut file)
        .headers()
        .context("reading headers")?
        .clone();

    let read_matrix =
        |file: &mut File, start_line: usize, end_line: usize| -> anyhow::Result<Array2<String>> {
            let byte_range =
                newlines.line2range(start_line).start..newlines.line2range(end_line).end;
            let rdr = take_range(file, byte_range)?;
            let mut rdr = csv::ReaderBuilder::new()
                .trim(csv::Trim::All)
                .from_reader(rdr);
            Ok(rdr.deserialize_array2::<String>((end_line - start_line, hdrs.len()))?)
        };

    let (mut cols, mut rows) = terminal::size()?;
    let mut start_line = 0usize;
    let mut start_col = 0usize;
    let mut msgs = String::new();
    let mut last_search = String::new();
    let mut exclude = vec![];
    let mut drawer = GridDrawer::default();

    loop {
        let end_line = min(newlines.len() - 2, start_line + rows as usize - 2);
        let matrix = read_matrix(&mut file, start_line, end_line).context("read matrix")?;

        drawer.draw(
            stdout,
            &mut df,
            DrawParams {
                rows: rows as usize,
                cols: cols as usize,
                start_line,
                end_line,
                start_col,
            },
            &exclude,
        )?;

        #[derive(Clone, Copy)]
        enum Mode {
            Jump,
            Search,
            Exclude,
        }
        let mut input_buf = String::new();
        let mut mode = Mode::Jump;
        loop {
            let prompt = match mode {
                Mode::Jump => ":",
                Mode::Search => "/",
                Mode::Exclude => "-",
            };
            stdout
                .queue(cursor::MoveTo(0, rows))?
                .queue(terminal::Clear(terminal::ClearType::CurrentLine))?
                .queue(style::Print(&prompt))?
                .queue(style::Print(&input_buf))?
                .queue(cursor::MoveTo(cols - msgs.len() as u16, rows))?
                .queue(style::SetForegroundColor(style::Color::Blue))?
                .queue(style::Print(&msgs.trim()))?
                .queue(style::ResetColor)?
                .queue(cursor::MoveTo(
                    (prompt.len() + input_buf.len()) as u16,
                    rows,
                ))?;
            stdout.flush()?;

            let max_line = newlines.len() - 2;
            let add = |start_line: usize, x: usize| min(max_line, start_line.saturating_add(x));
            let mut do_search = || {
                if !input_buf.is_empty() {
                    last_search = input_buf.clone();
                }
                let y = start_line + if input_buf.is_empty() { 2 } else { 1 };
                let x = newlines.line2range(y).start;
                file.seek(SeekFrom::Start(x))?;
                let matcher = grep_regex::RegexMatcher::new(&last_search)?;
                msgs.clear();
                msgs.push_str("No match");
                let sink = grep_searcher::sinks::UTF8(|line, _| {
                    msgs.clear();
                    start_line = add(y - 1, line as usize - 1);
                    Ok(false)
                });
                grep_searcher::Searcher::new().search_file(&matcher, &file, sink)?;
                anyhow::Result::<_>::Ok(())
            };
            use crossterm::event::{Event::*, KeyCode::*, KeyEvent, KeyModifiers};
            match (mode, event::read()?) {
                (
                    _,
                    Key(KeyEvent {
                        code: Backspace, ..
                    }),
                ) => {
                    if !input_buf.is_empty() {
                        input_buf.pop();
                        continue;
                    }
                }
                (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('-'), ..
                    }),
                ) => {
                    mode = Mode::Exclude;
                    continue;
                }
                (Mode::Exclude, Key(KeyEvent { code: Char(x), .. })) => {
                    input_buf.push(x);
                    continue; // Don't redraw
                }
                (Mode::Exclude, Key(KeyEvent { code: Enter, .. })) => {
                    exclude.push(input_buf.clone());
                }
                (Mode::Search, Key(KeyEvent { code: Char(x), .. })) => {
                    input_buf.push(x);
                    continue; // Don't redraw
                }
                (Mode::Search, Key(KeyEvent { code: Esc, .. })) => (), // leave search mode
                (Mode::Search, Key(KeyEvent { code: Enter, .. })) => {
                    do_search()?;
                }
                (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('n'),
                        modifiers: KeyModifiers::NONE,
                    }),
                ) => {
                    do_search()?;
                }
                (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('/'), ..
                    }),
                ) => {
                    mode = Mode::Search;
                    continue;
                }
                (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('?'), ..
                    }),
                ) => {
                    msgs.clear();
                    msgs.push_str("reverse search not implemented yet");
                }
                (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('n'),
                        modifiers: KeyModifiers::SHIFT,
                    }),
                ) => {
                    msgs.clear();
                    msgs.push_str("reverse search not implemented yet");
                }
                (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char(x @ '0'..='9'),
                        ..
                    }),
                ) => {
                    input_buf.push(x);
                    continue; // Don't redraw
                }
                (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('g'), ..
                    }),
                )
                | (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('G'), ..
                    }),
                )
                | (Mode::Jump, Key(KeyEvent { code: Enter, .. })) => {
                    match input_buf.parse::<usize>() {
                        Err(_) => (),
                        Ok(0) => start_line = 0,
                        Ok(x) => start_line = min(max_line, x - 1),
                    }
                }
                (Mode::Jump, Key(KeyEvent { code: Esc, .. }))
                | (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('c'),
                        modifiers: KeyModifiers::CONTROL,
                    }),
                )
                | (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('q'), ..
                    }),
                ) => return Ok(()),
                (Mode::Jump, Key(KeyEvent { code: Down, .. }))
                | (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('j'), ..
                    }),
                ) => start_line = add(start_line, 1),
                (Mode::Jump, Key(KeyEvent { code: Up, .. }))
                | (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('k'), ..
                    }),
                ) => start_line = start_line.saturating_sub(1),
                (Mode::Jump, Key(KeyEvent { code: PageDown, .. })) => {
                    start_line = add(start_line, rows as usize - 2)
                }
                (Mode::Jump, Key(KeyEvent { code: PageUp, .. })) => {
                    start_line = start_line.saturating_sub(rows as usize - 2)
                }
                (Mode::Jump, Key(KeyEvent { code: Home, .. })) => start_line = 0,
                (Mode::Jump, Key(KeyEvent { code: End, .. })) => start_line = max_line,
                (Mode::Jump, Key(KeyEvent { code: Right, .. }))
                | (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('l'), ..
                    }),
                ) => start_col += 1,
                (Mode::Jump, Key(KeyEvent { code: Left, .. }))
                | (
                    Mode::Jump,
                    Key(KeyEvent {
                        code: Char('h'), ..
                    }),
                ) => start_col = start_col.saturating_sub(1),
                (_, Resize(c, r)) => {
                    cols = c;
                    rows = r
                }
                _ => continue, // Don't redraw
            }
            break;
        }
    }
}
