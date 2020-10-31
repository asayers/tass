use anyhow::{bail, Context};
use crossterm::*;
use ndarray::prelude::*;
use ndarray_csv::Array2Reader;
use pad::PadStr;
use std::cmp::min;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use structopt::StructOpt;

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
    let mut file = match opts.path {
        Some(path) => File::open(&path)?,
        None => {
            let stdin = std::io::stdin();
            let mut stdin = stdin.lock();
            let mut file = tempfile::tempfile().context("creating tempfile")?;
            let bytes = std::io::copy(&mut stdin, &mut file).context("filling tempfile")?;
            eprintln!("Copied {} KB from stdin", bytes / 1024);
            file.seek(SeekFrom::Start(0))?;
            file
        }
    };

    let newlines = LineOffsets::new(&mut file).context("generating offsets")?;
    match newlines.len() {
        0 => bail!("No header row!"),
        1 => bail!("No data!"), // FIXME
        _ => (),
    }

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    // Set up terminal
    terminal::enable_raw_mode().context("entering raw mode")?;
    stdout.queue(terminal::EnterAlternateScreen)?.flush()?;

    // Store the result so the cleanup happens even if there's an error
    let result = main_3(newlines, file, &mut stdout);

    // Clean up terminal
    stdout.queue(terminal::LeaveAlternateScreen)?.flush()?;
    terminal::disable_raw_mode()?;
    result
}

fn take_range(file: &mut File, r: Range<u64>) -> std::io::Result<impl Read + '_> {
    file.seek(SeekFrom::Start(r.start))?;
    Ok(file.take(r.end - r.start))
}

struct LineOffsets(Vec<usize>);
impl LineOffsets {
    fn new(file: &mut File) -> anyhow::Result<LineOffsets> {
        eprint!("Gathering line breaks...");
        let ts = std::time::Instant::now();
        let newlines = LineOffsets::scan(file)?;
        let d = ts.elapsed();
        file.seek(SeekFrom::Start(0))?;
        eprintln!(" done! (Scanned {} lines in {:?})", newlines.len(), d);
        Ok(LineOffsets(newlines))
    }
    #[cfg(feature = "memmap")]
    fn scan(file: &mut File) -> anyhow::Result<Vec<usize>> {
        unsafe {
            eprint!(" creating mmap...");
            let mmap = memmap::Mmap::map(file)?;
            Ok(memchr::memchr_iter(b'\n', &mmap).collect::<Vec<_>>())
        }
    }
    #[cfg(not(feature = "memmap"))]
    fn scan(file: &mut File) -> anyhow::Result<Vec<usize>> {
        use std::io::{BufRead, BufReader};
        let mut file = BufReader::new(file);
        let mut offset = 0;
        let mut newlines = vec![];
        loop {
            let buf = file.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            if let Some(x) = memchr::memchr(b'\n', buf) {
                newlines.push(offset + x);
                offset += x + 1;
                file.consume(x + 1);
            } else {
                let x = buf.len();
                offset += x;
                file.consume(x);
            }
        }
        Ok(newlines)
    }
    /// Gives a byte-range which doesn't include the newline
    fn line2range(&self, line: usize) -> Range<u64> {
        self.0[line] as u64 + 1..self.0[line + 1] as u64
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

fn main_3(newlines: LineOffsets, mut file: File, stdout: &mut impl Write) -> anyhow::Result<()> {
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
    loop {
        let end_line = min(newlines.len() - 2, start_line + rows as usize - 2);
        let matrix = read_matrix(&mut file, start_line, end_line).context("read matrix")?;

        draw(
            stdout,
            rows as usize,
            cols as usize,
            start_line,
            start_col,
            &hdrs,
            &matrix,
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

fn draw(
    stdout: &mut impl Write,
    rows: usize,
    cols: usize,
    start_line: usize,
    start_col: usize,
    hdrs: &csv::StringRecord,
    matrix: &Array2<String>,
    exclude: &[String],
) -> anyhow::Result<()> {
    // Compute the widths
    let end_line = start_line + matrix.len_of(Axis(0)) - 1;
    let linnums_len = end_line.to_string().len() + 1;
    let mut budget = cols - linnums_len;
    let widths = std::iter::repeat(0)
        .take(start_col)
        .chain(hdrs.iter().enumerate().skip(start_col).map(|(i, hdr)| {
            let len = if exclude.iter().any(|x| x == hdr) {
                hdr.len()
            } else {
                std::iter::once(hdr)
                    .chain(matrix.column(i).into_iter().map(|x| x.as_str()))
                    .map(|x| x.len())
                    .max()
                    .unwrap()
            };
            let x = min(budget, len + 2);
            budget = budget.saturating_sub(len + 2);
            x
        }))
        .collect::<Vec<_>>();

    stdout.queue(terminal::Clear(terminal::ClearType::All))?;

    // Print the headers
    stdout
        .queue(cursor::MoveTo(0, 0))?
        .queue(style::SetAttribute(style::Attribute::Underlined))?
        .queue(style::SetAttribute(style::Attribute::Dim))?
        .queue(style::Print(" ".repeat(linnums_len - 1)))?
        .queue(style::Print(format!("│")))?
        .queue(style::SetAttribute(style::Attribute::Reset))?
        .queue(style::SetAttribute(style::Attribute::Underlined))?
        .queue(style::SetAttribute(style::Attribute::Bold))?
        .queue(style::SetForegroundColor(style::Color::Yellow))?;
    for (field, w) in hdrs.iter().zip(&widths) {
        if *w > 0 {
            stdout
                .queue(style::Print(" "))?
                .queue(style::Print(field.with_exact_width(*w - 1)))?;
        }
    }
    stdout.queue(style::ResetColor)?;

    // Print the body
    for (i, row) in matrix.outer_iter().enumerate() {
        stdout
            .queue(cursor::MoveToNextLine(1))?
            .queue(style::SetAttribute(style::Attribute::Dim))?
            .queue(style::Print(format!(
                "{:>w$}│",
                i + start_line + 1,
                w = linnums_len - 1
            )))?
            .queue(style::SetAttribute(style::Attribute::Reset))?;
        for (field, w) in row.iter().zip(&widths) {
            if *w > 0 {
                stdout
                    .queue(style::Print(" "))?
                    .queue(style::Print(field.with_exact_width(*w - 1)))?;
            }
        }
    }

    stdout.queue(style::SetForegroundColor(style::Color::Blue))?;
    for _ in 0..rows.saturating_sub(matrix.len_of(Axis(0))) {
        stdout
            .queue(cursor::MoveToNextLine(1))?
            .queue(style::Print("~"))?;
    }
    stdout.queue(style::ResetColor)?;

    Ok(())
}
