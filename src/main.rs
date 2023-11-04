mod csv;
mod draw;
mod parquet;
mod prompt;
mod stats;

use crate::csv::*;
use crate::draw::*;
use crate::parquet::*;
use crate::prompt::*;
use crate::stats::*;
use anyhow::bail;
use anyhow::Context;
use arrow::record_batch::RecordBatch;
use bpaf::{Bpaf, Parser};
use crossterm::tty::IsTty;
use crossterm::*;
use std::fs::File;
use std::io::LineWriter;
use std::io::Write;
use std::ops::Range;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::debug;

/// A pager for tabular data
#[derive(Bpaf)]
struct Opts {
    #[bpaf(positional)]
    path: Option<PathBuf>,
    /// How many decimal places to show when rendering floating-point numbers
    #[bpaf(fallback(5))]
    precision: usize,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let opts = opts().run();
    let settings = RenderSettings {
        float_dps: opts.precision,
    };

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
    let source: Box<dyn DataSource> = match ext {
        Some("parquet") => Box::new(ParquetFile::new(file)?),
        Some("csv") => Box::new(CsvFile::new(file)?),
        None => Box::new(CsvFile::new(file)?),
        _ => bail!("Unrecognised file extension"),
    };
    let source = CachedSource::new(source, &settings)?;

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    // Set up terminal
    terminal::enable_raw_mode().context("entering raw mode")?;
    stdout
        .queue(terminal::EnterAlternateScreen)?
        .queue(terminal::DisableLineWrap)?
        .flush()?;

    // Store the result so the cleanup happens even if there's an error
    let result = runloop(&mut stdout, source, settings);

    // Clean up terminal
    stdout
        .queue(terminal::EnableLineWrap)?
        .queue(terminal::LeaveAlternateScreen)?
        .flush()?;
    terminal::disable_raw_mode()?;
    result
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

trait DataSource {
    fn row_count(&mut self) -> anyhow::Result<usize>;
    fn fetch_batch(&mut self, offset: usize, len: usize) -> anyhow::Result<RecordBatch>;
    fn search(&mut self, needle: &str, from: usize, rev: bool) -> anyhow::Result<Option<usize>>;
}

impl CachedSource {
    fn new(mut source: Box<dyn DataSource>, settings: &RenderSettings) -> anyhow::Result<Self> {
        let start = Instant::now();
        let big_df = source.fetch_batch(0, CHUNK_SIZE)?;
        debug!(
            "Loaded initial batch: {} MiB (took {:?})",
            big_df.get_array_memory_size() / 1024 / 1024,
            start.elapsed(),
        );

        let all_col_stats = big_df
            .schema()
            .fields()
            .iter()
            .zip(big_df.columns())
            .map(|(field, col)| ColumnStats::new(&field.name(), col, settings))
            .collect::<anyhow::Result<Vec<ColumnStats>>>()?;

        let (available_cols, col_stats): (Vec<_>, Vec<_>) = big_df
            .columns()
            .into_iter()
            .enumerate()
            .filter(|(_, col)| col.null_count() < col.len())
            .map(|(idx, _)| (idx, all_col_stats[idx].clone()))
            .unzip();

        debug!(
            "Hid {} empty columns",
            big_df.num_columns() - available_cols.len(),
        );

        Ok(CachedSource {
            inner: source,
            all_col_stats,
            big_df,
            available_rows: 0..CHUNK_SIZE,
            available_cols,
            col_stats,
        })
    }

    fn get_batch(
        &mut self,
        rows: Range<usize>,
        mut cols: Range<usize>,
        settings: &RenderSettings,
    ) -> anyhow::Result<RecordBatch> {
        let all_rows_available =
            self.available_rows.contains(&rows.start) && self.available_rows.contains(&rows.end);
        if !all_rows_available {
            debug!("Requested: {rows:?}; available: {:?}", self.available_rows);
            let start = Instant::now();
            let from = rows.start.saturating_sub(CHUNK_SIZE / 2);
            self.big_df = self.inner.fetch_batch(from, CHUNK_SIZE)?;
            self.available_rows = from..(from + CHUNK_SIZE);
            debug!(took=?start.elapsed(),
                "Loaded a new batch (rows {:?}, {} MiB)",
                self.available_rows,
                self.big_df.get_array_memory_size() / 1024 / 1024,
            );

            let start = Instant::now();
            for ((field, old_stats), col) in self
                .big_df
                .schema()
                .fields()
                .iter()
                .zip(self.all_col_stats.iter_mut())
                .zip(self.big_df.columns())
            {
                let new_stats = ColumnStats::new(&field.name(), col, settings)?;
                old_stats.merge(new_stats);
            }
            self.col_stats.clear();
            self.available_cols.clear();
            for (idx, col) in self.big_df.columns().into_iter().enumerate() {
                if col.null_count() < col.len() {
                    self.available_cols.push(idx);
                    self.col_stats.push(self.all_col_stats[idx].clone());
                }
            }
            debug!(took=?start.elapsed(), "Refined the schema");

            cols.start = cols.start.min(self.available_cols.len());
            cols.end = cols.end.min(self.available_cols.len());
        }

        let enabled_cols = &self.available_cols[cols];
        let offset = rows.start - self.available_rows.start;
        let len = rows.end - rows.start;
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

    loop {
        let total_rows = source.inner.row_count()?;
        let idx_width = if total_rows == 0 {
            0
        } else {
            total_rows.ilog10() as u16
        } + 1;
        let end_row = (start_row + term_size.1 as usize - 2).min(total_rows.saturating_sub(1));
        let end_col = source.col_stats[start_col..]
            .iter()
            .scan(idx_width, |acc, x| {
                *acc += x.width + 3;
                Some(*acc)
            })
            .position(|x| x > term_size.0)
            .map(|x| x as usize + start_col + 1)
            .unwrap_or(source.col_stats.len());
        // TODO: Reduce the width of the final column
        let batch = source.get_batch(start_row..end_row, start_col..end_col, &settings)?;
        draw(
            stdout,
            start_row,
            batch,
            term_size.1,
            idx_width,
            &source.col_stats[start_col..end_col],
            &settings,
            &prompt,
        )?;

        if event::poll(Duration::from_millis(1000))? {
            let event = event::read()?;
            match event {
                event::Event::Key(k) => {
                    let cmd = match k.code {
                        event::KeyCode::Char('c')
                            if k.modifiers.contains(event::KeyModifiers::CONTROL) =>
                        {
                            return Ok(())
                        }
                        code => prompt.handle(code),
                    };
                    if let Some(cmd) = cmd {
                        match cmd {
                            Cmd::ColRight => {
                                start_col =
                                    (start_col + 1).min(source.col_stats.len().saturating_sub(1))
                            }
                            Cmd::ColLeft => start_col = start_col.saturating_sub(1),
                            Cmd::RowDown => {
                                start_row = (start_row + 1).min(total_rows.saturating_sub(2))
                            }
                            Cmd::RowUp => start_row = start_row.saturating_sub(1),
                            Cmd::RowBottom => start_row = total_rows.saturating_sub(2),
                            Cmd::RowTop => start_row = 0,
                            Cmd::RowPgUp => {
                                start_row = start_row.saturating_sub(term_size.1 as usize - 2)
                            }
                            Cmd::RowPgDown => {
                                start_row = (start_row + term_size.1 as usize - 2)
                                    .min(total_rows.saturating_sub(2))
                            }
                            Cmd::RowGoTo(x) => start_row = x.min(total_rows.saturating_sub(2)),
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
                            Cmd::Exit => return Ok(()),
                        }
                    }
                }
                event::Event::Resize(cols, rows) => term_size = (cols, rows),
                _ => (),
            }
        }
    }
}
