mod dataframe;
mod grid;

use crate::dataframe::*;
use crate::grid::*;
use anyhow::Context;
use crossterm::*;
use std::cmp::{max, min};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use structopt::StructOpt;
use tempfile::*;

/// A pager for tabular data
#[derive(StructOpt)]
struct Opts {
    path: Option<PathBuf>,
    /// Start in follow mode
    #[structopt(short, long)]
    follow: bool,
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

    let df = DataFrame::new(path.as_ref().as_ref()).context("loading dataframe")?;

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    // Set up terminal
    terminal::enable_raw_mode().context("entering raw mode")?;
    stdout.queue(terminal::EnterAlternateScreen)?.flush()?;

    // Store the result so the cleanup happens even if there's an error
    let result = main_3(df, opts.follow, &mut stdout);

    // Clean up terminal
    stdout.queue(terminal::LeaveAlternateScreen)?.flush()?;
    terminal::disable_raw_mode()?;
    result
}

fn main_3(mut df: DataFrame, start_in_follow: bool, stdout: &mut impl Write) -> anyhow::Result<()> {
    let (mut cols, mut rows) = terminal::size()?;
    let mut start_line = 0usize;
    let mut start_col = 0usize;
    let mut msgs = String::new();
    let mut last_search = String::new();
    let mut exclude = vec![];
    let mut drawer = GridDrawer::default();

    #[derive(Clone, Copy, PartialEq)]
    enum Mode {
        Jump,
        Search,
        Exclude,
        Follow,
    }
    let mut input_buf = String::new();
    let mut mode = if start_in_follow {
        Mode::Follow
    } else {
        Mode::Jump
    };

    loop {
        if mode == Mode::Follow {
            start_line = max(0, df.len().saturating_sub(rows as usize));
        }
        let end_line = min(df.len() - 2, start_line + rows as usize - 2);
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

        let position = format!("{}-{} of {}", start_line + 1, end_line, df.len() - 2);
        let prompt = match mode {
            Mode::Jump => ": ",
            Mode::Search => "/ ",
            Mode::Exclude => "- ",
            Mode::Follow => "> ",
        };
        stdout
            .queue(cursor::MoveTo(0, rows))?
            .queue(terminal::Clear(terminal::ClearType::CurrentLine))?
            .queue(style::Print(&position))?
            .queue(style::Print(&prompt))?
            .queue(style::Print(&input_buf))?
            .queue(cursor::MoveTo(cols - msgs.len() as u16, rows))?
            .queue(style::SetForegroundColor(style::Color::Blue))?
            .queue(style::Print(&msgs.trim()))?
            .queue(style::ResetColor)?
            .queue(cursor::MoveTo(
                (position.len() + prompt.len() + input_buf.len()) as u16,
                rows,
            ))?;
        stdout.flush()?;

        // TODO: Get a prompt notification of file change, don't poll
        if !event::poll(Duration::from_millis(100))? {
            df.refresh()?;
            continue;
        }

        // We have user input; let's handle it

        msgs.clear();
        let max_line = df.len() - 2;
        let add = |start_line: usize, x: usize| min(max_line, start_line.saturating_add(x));
        let key = match event::read()? {
            Key(k) => k,
            Resize(c, r) => {
                cols = c;
                rows = r;
                continue;
            }
            Mouse(_) => continue,
        };
        use crossterm::event::{Event::*, KeyCode::*, KeyModifiers};
        use Mode::*;
        match (mode, key.code) {
            // Exiting the program
            (Jump, Esc) | (Jump, Char('q')) | (Follow, Char('q')) => return Ok(()),
            (_, Char('c')) if key.modifiers == KeyModifiers::CONTROL => return Ok(()),

            // Typing at the prompt (search/exclude modes)
            (Search, Char(x)) | (Exclude, Char(x)) => input_buf.push(x),
            (Search, Backspace) | (Exclude, Backspace) => {
                if input_buf.is_empty() {
                    mode = Mode::Jump
                } else {
                    input_buf.pop();
                }
            }
            (Search, Esc) | (Exclude, Esc) => {
                input_buf.clear();
                mode = Jump;
            }

            // Exclude mode
            (Jump, Char('-')) => mode = Exclude,
            (Exclude, Enter) => {
                exclude.push(input_buf.clone());
                input_buf.clear();
                mode = Jump;
            }

            // Search mode
            (Jump, Char('/')) => mode = Search,
            (Search, Enter) => {
                std::mem::swap(&mut last_search, &mut input_buf);
                input_buf.clear();
                mode = Jump;
                match df.search(start_line + 1, &last_search)? {
                    Some(line) => start_line = line,
                    None => msgs.push_str("No match"),
                }
            }
            (Jump, Char('n')) => match df.search(start_line + 2, &last_search)? {
                Some(line) => start_line = line,
                None => msgs.push_str("No match"),
            },

            (Jump, Char('?')) => msgs.push_str("reverse search not implemented yet"),
            (Jump, Char('N')) => msgs.push_str("reverse search not implemented yet"),

            (Jump, Char(x @ '0'..='9')) => input_buf.push(x),
            (Jump, Char('g')) | (Jump, Char('G')) | (Jump, Enter) => {
                match input_buf.parse::<usize>() {
                    Err(_) => (),
                    Ok(0) => start_line = 0,
                    Ok(x) => start_line = min(max_line, x - 1),
                }
                input_buf.clear();
            }

            // Scrolling the grid
            (Jump, Down) | (Jump, Char('j')) => start_line = add(start_line, 1),
            (Jump, Up) | (Jump, Char('k')) => start_line = start_line.saturating_sub(1),
            (Jump, PageDown) => start_line = add(start_line, rows as usize - 2),
            (Jump, PageUp) => start_line = start_line.saturating_sub(rows as usize - 2),
            (Jump, Home) => start_line = 0,
            (Jump, End) => start_line = max_line,
            (Jump, Right) | (Jump, Char('l')) | (Follow, Right) | (Follow, Char('l')) => {
                start_col += 1
            }
            (Jump, Left) | (Jump, Char('h')) | (Follow, Left) | (Follow, Char('h')) => {
                start_col = start_col.saturating_sub(1)
            }

            // Follow mode: 'f' to enter, anything else leaves
            (Jump, Char('f')) | (Jump, Char('F')) => mode = Follow,
            (Follow, _) => mode = Jump,

            _ => (),
        }
    }
}
