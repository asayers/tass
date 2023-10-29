mod draw;
mod stats;

use crate::draw::*;
use crate::stats::*;
use anyhow::Context;
use arrow::record_batch::RecordBatch;
use bpaf::{Bpaf, Parser};
use crossterm::*;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// A pager for tabular data
#[derive(Bpaf)]
struct Opts {
    #[bpaf(positional)]
    path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let opts = opts().run();

    let foo = Foo::new(&opts.path)?;

    // let mut n = 0;
    // for null_count in lf.clone().null_count().collect()?.get_columns() {
    //     let n_nulls = null_count.u32()?.get(0).unwrap() as usize;
    //     if n_nulls == total_rows {
    //         lf = lf.drop_columns([null_count.name()]);
    //         n += 1;
    //     }
    // }
    // eprintln!("Hid {n} empty columns");

    // lf       .schema()?
    //        .iter_fields()
    //        .map(|x| ColumnStats::new(&lf, &x.name, x.dtype))
    //        .collect::<anyhow::Result<Vec<ColumnStats>>>()?;

    // The width of the widest value in each column, when formatted (including the header)

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    // Set up terminal
    terminal::enable_raw_mode().context("entering raw mode")?;
    stdout
        .queue(terminal::EnterAlternateScreen)?
        .queue(terminal::DisableLineWrap)?
        .flush()?;

    // Store the result so the cleanup happens even if there's an error
    let result = runloop(&mut stdout, foo);

    // Clean up terminal
    stdout
        .queue(terminal::EnableLineWrap)?
        .queue(terminal::LeaveAlternateScreen)?
        .flush()?;
    terminal::disable_raw_mode()?;
    result
}

const CHUNK_SIZE: usize = 10_000;

struct Foo {
    file: File,
    total_rows: usize,
    available_rows: Range<usize>, // The rows in big_df
    big_df: RecordBatch,
    col_stats: Vec<ColumnStats>, // One per column
    idx_width: u16,
}

fn count_rows(file: File) -> anyhow::Result<usize> {
    let start = Instant::now();
    let rdr = SerializedFileReader::new(file)?;
    let total_rows = rdr.metadata().file_metadata().num_rows() as usize;
    eprintln!("Counted {total_rows} rows (took {:?})", start.elapsed());
    Ok(total_rows)
}

fn fetch_batch(file: File, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
    let start = Instant::now();
    let mut rdr = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(len)
        .with_row_selection(
            vec![
                RowSelector {
                    row_count: offset,
                    skip: true,
                },
                RowSelector {
                    row_count: len,
                    skip: false,
                },
            ]
            .into(),
        )
        .build()?;
    let batch = rdr.next().unwrap()?;
    eprintln!(
        "Loaded a new batch: {} MiB (took {:?})",
        batch.get_array_memory_size() / 1024 / 1024,
        start.elapsed(),
    );
    Ok(batch)
}

impl Foo {
    fn new(path: &Path) -> anyhow::Result<Self> {
        let file = File::open(path)?;

        let total_rows = count_rows(file.try_clone()?)?;

        let big_df = fetch_batch(file.try_clone()?, 0, CHUNK_SIZE)?;

        let n_cols = big_df.schema().fields().len();
        let col_stats = vec![ColumnStats::default(); n_cols];
        let idx_width = total_rows.ilog10() as u16 + 1;
        Ok(Foo {
            file,
            total_rows,
            available_rows: 0..0,
            big_df,
            col_stats,
            idx_width,
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
            let from = start_row.saturating_sub(1000);
            self.big_df = fetch_batch(self.file.try_clone()?, from, CHUNK_SIZE)?;
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
        let enabled_cols: Vec<usize> = (start_col..self.big_df.num_columns()).collect();
        let offset = start_row - self.available_rows.start;
        let len = (term_size.1 as usize - 2).min(self.big_df.num_rows() - offset);
        let mini_df = self.big_df.project(&enabled_cols)?.slice(offset, len);
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

fn runloop(stdout: &mut impl Write, mut foo: Foo) -> anyhow::Result<()> {
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
