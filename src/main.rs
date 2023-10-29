mod draw;
mod parquet;
mod stats;

use crate::draw::*;
use crate::parquet::*;
use crate::stats::*;
use anyhow::bail;
use anyhow::Context;
use arrow::record_batch::RecordBatch;
use bpaf::{Bpaf, Parser};
use crossterm::*;
use std::fs::File;
use std::io::Write;
use std::ops::Range;
use std::path::PathBuf;
use std::time::Duration;

/// A pager for tabular data
#[derive(Bpaf)]
struct Opts {
    #[bpaf(positional)]
    path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let opts = opts().run();

    let file = match opts.path.extension().and_then(|x| x.to_str()) {
        Some("parquet") => Box::new(ParquetFile::new(File::open(&opts.path)?)),
        _ => bail!("Unrecognised file extension"),
    };
    let state = State::new(file)?;

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    // Set up terminal
    terminal::enable_raw_mode().context("entering raw mode")?;
    stdout
        .queue(terminal::EnterAlternateScreen)?
        .queue(terminal::DisableLineWrap)?
        .flush()?;

    // Store the result so the cleanup happens even if there's an error
    let result = runloop(&mut stdout, state);

    // Clean up terminal
    stdout
        .queue(terminal::EnableLineWrap)?
        .queue(terminal::LeaveAlternateScreen)?
        .flush()?;
    terminal::disable_raw_mode()?;
    result
}

const CHUNK_SIZE: usize = 10_000;

struct State {
    file: Box<dyn DataSource>,
    total_rows: usize,
    available_rows: Range<usize>, // The rows in big_df
    big_df: RecordBatch,
    col_stats: Vec<ColumnStats>, // One per column
    idx_width: u16,
    col_idxs: Vec<usize>,
}

trait DataSource {
    fn count_rows(&self) -> anyhow::Result<usize>;
    fn fetch_batch(&self, offset: usize, len: usize) -> anyhow::Result<RecordBatch>;
}

impl State {
    fn new(file: Box<dyn DataSource>) -> anyhow::Result<Self> {
        let total_rows = file.count_rows()?;
        let idx_width = total_rows.ilog10() as u16 + 1;

        let big_df = file.fetch_batch(0, CHUNK_SIZE)?;
        let n_cols = big_df.num_columns();

        let col_stats = big_df
            .schema()
            .fields()
            .iter()
            .zip(big_df.columns())
            .map(|(field, col)| ColumnStats::new(&field.name(), col))
            .collect::<anyhow::Result<Vec<ColumnStats>>>()?;

        // let mut n = 0;
        // for null_count in lf.clone().null_count().collect()?.get_columns() {
        //     let n_nulls = null_count.u32()?.get(0).unwrap() as usize;
        //     if n_nulls == total_rows {
        //         lf = lf.drop_columns([null_count.name()]);
        //         n += 1;
        //     }
        // }
        // eprintln!("Hid {n} empty columns");

        Ok(State {
            file,
            total_rows,
            available_rows: 0..CHUNK_SIZE,
            big_df,
            col_stats,
            idx_width,
            col_idxs: (0..n_cols).collect(),
        })
    }

    fn update_df(
        &mut self,
        start_row: usize,
        start_col: usize,
        term_size: (u16, u16),
    ) -> anyhow::Result<RecordBatch> {
        let end_row = (start_row + term_size.1 as usize - 2).min(self.total_rows - 1);
        let all_rows_available =
            self.available_rows.contains(&start_row) && self.available_rows.contains(&end_row);
        if !all_rows_available {
            let from = start_row.saturating_sub(CHUNK_SIZE / 2);
            self.big_df = self.file.fetch_batch(from, CHUNK_SIZE)?;
            self.available_rows = from..(from + CHUNK_SIZE);
            for ((field, old_stats), col) in self
                .big_df
                .schema()
                .fields()
                .iter()
                .zip(self.col_stats.iter_mut())
                .zip(self.big_df.columns())
            {
                let new_stats = ColumnStats::new(&field.name(), col)?;
                old_stats.merge(new_stats);
            }
        }
        let enabled_cols = &self.col_idxs[start_col..];
        let offset = start_row - self.available_rows.start;
        let len = (term_size.1 as usize - 2).min(self.big_df.num_rows() - offset);
        let mini_df = self.big_df.project(enabled_cols)?.slice(offset, len);
        Ok(mini_df)
    }

    fn draw(
        &mut self,
        stdout: &mut impl Write,
        start_row: usize,
        start_col: usize,
        term_size: (u16, u16),
    ) -> anyhow::Result<()> {
        let mini_df = self.update_df(start_row, start_col, term_size)?;
        draw(
            stdout,
            start_row,
            mini_df.columns(),
            term_size,
            self.idx_width,
            &self.col_stats[start_col..],
        )
    }
}

fn runloop(stdout: &mut impl Write, mut foo: State) -> anyhow::Result<()> {
    let mut term_size = terminal::size()?;
    let mut start_col: usize = 0;
    let mut start_row: usize = 0;

    loop {
        foo.draw(stdout, start_row, start_col, term_size)?;
        if event::poll(Duration::from_millis(1000))? {
            let event = event::read()?;
            match event {
                event::Event::Key(k) => match k.code {
                    event::KeyCode::Char('c')
                        if k.modifiers.contains(event::KeyModifiers::CONTROL) =>
                    {
                        return Ok(())
                    }
                    event::KeyCode::Esc | event::KeyCode::Char('q') => return Ok(()),
                    event::KeyCode::Right | event::KeyCode::Char('l') => start_col += 1,
                    event::KeyCode::Left | event::KeyCode::Char('h') => {
                        start_col = start_col.saturating_sub(1)
                    }
                    event::KeyCode::Down | event::KeyCode::Char('j') => {
                        start_row = (start_row + 1).min(foo.total_rows - 1)
                    }
                    event::KeyCode::Up | event::KeyCode::Char('k') => {
                        start_row = start_row.saturating_sub(1)
                    }
                    event::KeyCode::End | event::KeyCode::Char('G') => {
                        start_row = foo.total_rows - 1
                    }
                    event::KeyCode::Home | event::KeyCode::Char('g') => start_row = 0,
                    event::KeyCode::PageUp => {
                        start_row = start_row.saturating_sub(term_size.1 as usize - 2)
                    }
                    event::KeyCode::PageDown => {
                        start_row = (start_row + term_size.1 as usize - 2).min(foo.total_rows - 1)
                    }
                    _ => (),
                },
                event::Event::Resize(cols, rows) => term_size = (cols, rows),
                _ => (),
            }
            foo.update_df(start_row, start_col, term_size)?;
        }
    }
}
