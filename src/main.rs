mod backend;
mod draw;
mod prompt;
mod stats;

use crate::backend::DataSource;
use crate::draw::*;
use crate::prompt::*;
use crate::stats::*;
use anyhow::bail;
#[cfg(feature = "virt")]
use anyhow::ensure;
use anyhow::Context;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bpaf::{Bpaf, Parser};
use crossterm::tty::IsTty;
use crossterm::*;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::{LineWriter, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::debug;

/// A pager for tabular data
///
/// Data can be in CSV or parquet format.  The format is inferred from the file
/// extension.  When data is read from stdin, it is expected to be CSV.
#[derive(Bpaf)]
struct Opts {
    /// How many decimal places to show when rendering floating-point numbers
    #[bpaf(fallback(5))]
    precision: usize,
    /// Whether to hide empty columns
    hide_empty: bool,
    /// A column to sort by. Prefix with '-' to invert
    #[cfg(feature = "virt")]
    sort: Option<String>,
    /// A filter expression, eg. 'age > 30'
    #[cfg(feature = "virt")]
    filter: Option<String>,
    /// The path to read.  If not specified, data will be read from stdin
    #[bpaf(positional)]
    path: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    init_logger();

    let opts = opts().run();
    let settings = RenderSettings {
        float_dps: opts.precision,
        hide_empty: opts.hide_empty,
    };

    let source = CachedSource::new(get_source(&opts)?);

    let stdout = std::io::stdout();
    let mut stdout = BufWriter::new(stdout.lock());

    // Set up terminal
    terminal::enable_raw_mode().context("entering raw mode")?;
    stdout
        .queue(terminal::EnterAlternateScreen)?
        .queue(terminal::DisableLineWrap)?
        .queue(event::EnableMouseCapture)?
        .flush()?;

    // Store the result so the cleanup happens even if there's an error
    let result = runloop(&mut stdout, source, settings);

    // Clean up terminal
    stdout
        .queue(event::DisableMouseCapture)?
        .queue(terminal::EnableLineWrap)?
        .queue(terminal::LeaveAlternateScreen)?
        .flush()?;
    terminal::disable_raw_mode()?;
    result
}

fn get_source(opts: &Opts) -> anyhow::Result<Box<dyn DataSource>> {
    #[cfg(feature = "virt")]
    if opts.sort.is_some() || opts.filter.is_some() {
        let Some(path) = &opts.path else {
            bail!("Can't filter streaming data")
        };
        let ext = path.extension().and_then(|x| x.to_str());
        ensure!(ext == Some("parquet"), "Can't filter this file type");
        let source = crate::backend::virt::VirtualFile::new(
            path,
            opts.sort.as_deref(),
            opts.filter.as_deref(),
        )?;
        return Ok(Box::new(source));
    }

    let (file, ext) = match &opts.path {
        Some(x) => (File::open(x)?, x.extension().and_then(|x| x.to_str())),
        None => {
            let mut stdin = std::io::stdin();
            if stdin.is_tty() {
                bail!("Need to specify a filename or feed data to stdin");
            }
            let tmpfile = tempfile::tempfile()?;
            let mut wtr = LineWriter::new(tmpfile.try_clone()?);
            std::thread::spawn(move || std::io::copy(&mut stdin, &mut wtr));
            (tmpfile, None)
        }
    };

    Ok(match ext {
        #[cfg(feature = "parquet")]
        Some("parquet") => Box::new(crate::backend::parquet::ParquetFile::new(file)?),
        Some("csv") => Box::new(crate::backend::csv::CsvFile::new(file)?),
        #[cfg(feature = "json")]
        Some("json" | "jsonl" | "ndjson") => Box::new(crate::backend::json::JsonFile::new(file)?),
        None => Box::new(crate::backend::csv::CsvFile::new(file)?),
        _ => bail!("Unrecognised file extension"),
    })
}

const CHUNK_SIZE: usize = 10_000;

struct CachedSource {
    inner: Box<dyn DataSource>,
    all_col_stats: Vec<ColumnStats>, // One per column
    // The below refer to the loaded record batch
    big_df: RecordBatch,
    available_cols: Vec<usize>,   // The columns in big_df
    available_rows: Range<usize>, // The rows in big_df
    col_stats: Vec<ColumnStats>,  // One per column in big_df
}

impl CachedSource {
    fn new(source: Box<dyn DataSource>) -> Self {
        CachedSource {
            inner: source,
            all_col_stats: vec![],
            big_df: RecordBatch::new_empty(Schema::empty().into()),
            available_rows: 0..0,
            available_cols: vec![],
            col_stats: vec![],
        }
    }

    /// If this returns `Ok`, the requested rows should now be available - ie.
    /// you can pass the range into `get_batch()`.
    fn ensure_available(
        &mut self,
        rows: Range<usize>,
        settings: &RenderSettings,
    ) -> anyhow::Result<()> {
        let all_rows_available = self.available_rows.contains(&rows.start)
            && self.available_rows.contains(&(rows.end - 1));
        if all_rows_available {
            return Ok(());
        }

        debug!("Requested: {rows:?}; available: {:?}", self.available_rows);
        let start = Instant::now();
        let from = rows.start.saturating_sub(CHUNK_SIZE / 2);
        self.big_df = self.inner.fetch_batch(from, CHUNK_SIZE)?;
        self.available_rows = from..(from + self.big_df.num_rows());
        debug!(took=?start.elapsed(),
            "Loaded a new batch (rows {:?}, {} MiB)",
            self.available_rows,
            self.big_df.get_array_memory_size() / 1024 / 1024,
        );

        let start = Instant::now();
        for (idx, (field, col)) in self
            .big_df
            .schema()
            .fields()
            .iter()
            .zip(self.big_df.columns())
            .enumerate()
        {
            let new_stats = ColumnStats::new(field.name(), col, settings)?;
            match idx.cmp(&self.all_col_stats.len()) {
                Ordering::Less => self.all_col_stats[idx].merge(new_stats),
                Ordering::Equal => self.all_col_stats.push(new_stats),
                Ordering::Greater => panic!(),
            }
        }
        self.col_stats.clear();
        self.available_cols.clear();
        for (idx, col) in self.big_df.columns().iter().enumerate() {
            if !settings.hide_empty || col.null_count() < col.len() {
                self.available_cols.push(idx);
                self.col_stats.push(self.all_col_stats[idx].clone());
            }
        }
        debug!(took=?start.elapsed(), "Refined the stats");
        Ok(())
    }

    fn get_batch(&self, rows: Range<usize>, cols: Range<usize>) -> anyhow::Result<RecordBatch> {
        debug!(?rows, ?cols, "Slicing big df");
        let enabled_cols = &self.available_cols[cols];
        let offset = rows.start - self.available_rows.start;
        let len = rows.end.min(self.available_rows.end) - rows.start;
        let mini_df = self.big_df.project(enabled_cols)?.slice(offset, len);
        Ok(mini_df)
    }
}

fn runloop(
    stdout: &mut impl Write,
    mut source: CachedSource,
    settings: RenderSettings,
) -> anyhow::Result<()> {
    let mut term_size = terminal::size()?;
    let mut start_col: usize = 0;
    let mut start_row: usize = 0;
    let mut prompt = Prompt::default();
    let mut file_refresh_interval = Duration::from_millis(10);
    let mut last_file_refresh = Instant::now();
    let mut total_rows = source.inner.row_count();
    let mut dirty = true;
    let mut col_widths = vec![];
    let mut highlights = HashSet::<usize>::default();

    // Load the initial batch
    source.ensure_available(0..0, &settings)?;

    loop {
        if last_file_refresh.elapsed() > file_refresh_interval {
            let new_rows = source.inner.check_for_new_rows()?;
            if new_rows == 0 {
                file_refresh_interval = (file_refresh_interval * 10).min(Duration::from_secs(1));
            } else {
                total_rows = source.inner.row_count();
                file_refresh_interval = Duration::from_millis(10);
                dirty = true;
            }
            last_file_refresh = Instant::now();
        }

        if dirty {
            let idx_width = if total_rows == 0 {
                0
            } else {
                total_rows.ilog10() as u16
            } + 1;

            if prompt.is_following() {
                start_row = total_rows.saturating_sub(term_size.1 as usize - 2);
            }
            let end_row = (start_row + term_size.1 as usize - 2).min(total_rows);
            let rows = start_row..end_row;
            source.ensure_available(rows.clone(), &settings)?;

            col_widths.clear();
            let mut remaining = term_size.0 - idx_width - 2;
            for stats in &source.col_stats[start_col..] {
                if remaining >= 1 {
                    let w = stats.ideal_width.min(remaining);
                    remaining = remaining.saturating_sub(3 + w);
                    col_widths.push(w);
                }
            }
            let end_col = start_col + col_widths.len();
            let cols = start_col..end_col;

            let batch = source.get_batch(rows, cols.clone())?;
            draw(
                stdout,
                start_row,
                batch,
                term_size.0,
                term_size.1,
                idx_width,
                &col_widths,
                total_rows,
                &source.col_stats[cols],
                &settings,
                &prompt,
                &highlights,
            )?;
            dirty = false;
        }

        if event::poll(file_refresh_interval)? {
            let event = event::read()?;
            let cmd = match event {
                event::Event::Key(k) => match k.code {
                    event::KeyCode::Char('c')
                        if k.modifiers.contains(event::KeyModifiers::CONTROL) =>
                    {
                        return Ok(())
                    }
                    code => prompt.handle_key(code),
                },
                event::Event::Mouse(ev) => prompt.handle_mouse(ev),
                event::Event::Resize(cols, rows) => {
                    term_size = (cols, rows);
                    None
                }
                _ => None,
            };
            if let Some(cmd) = cmd {
                match cmd {
                    Cmd::ColRight => {
                        start_col = (start_col + 1).min(source.col_stats.len().saturating_sub(1))
                    }
                    Cmd::ColLeft => start_col = start_col.saturating_sub(1),
                    Cmd::RowDown => start_row = (start_row + 1).min(total_rows.saturating_sub(1)),
                    Cmd::RowUp => start_row = start_row.saturating_sub(1),
                    Cmd::RowBottom => start_row = total_rows.saturating_sub(1),
                    Cmd::RowTop => start_row = 0,
                    Cmd::RowPgUp => start_row = start_row.saturating_sub(term_size.1 as usize - 2),
                    Cmd::RowPgDown => {
                        start_row =
                            (start_row + term_size.1 as usize - 2).min(total_rows.saturating_sub(1))
                    }
                    Cmd::RowGoTo(x) => start_row = x.min(total_rows.saturating_sub(1)),
                    Cmd::SearchNext(needle) => {
                        if let Some(x) = source.inner.search(&needle, start_row, false)? {
                            start_row = x;
                        }
                    }
                    Cmd::SearchPrev(needle) => {
                        if let Some(x) = source.inner.search(&needle, start_row, true)? {
                            start_row = x;
                        }
                    }
                    Cmd::ToggleHighlight(row) => {
                        let row = start_row + row as usize - 1;
                        if highlights.contains(&row) {
                            highlights.remove(&row);
                        } else {
                            highlights.insert(row);
                        }
                    }
                    Cmd::Exit => return Ok(()),
                }
            }
            dirty = true;
        }
    }
}

fn init_logger() {
    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(tracing_subscriber::filter::EnvFilter::from_default_env())
        .init();
}
