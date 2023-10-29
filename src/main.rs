mod draw;
mod stats;

use crate::draw::*;
use crate::stats::*;
use anyhow::Context;
use arrow::array::ArrayRef;
use bpaf::{Bpaf, Parser};
use crossterm::*;
use polars::prelude::*;
use std::io::Write;
use std::ops::Range;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// A pager for tabular data
#[derive(Bpaf)]
struct Opts {
    #[bpaf(positional)]
    path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let opts = opts().run();

    let start = Instant::now();
    let scan_args = ScanArgsParquet {
        parallel: ParallelStrategy::None,
        ..Default::default()
    };
    let lf = LazyFrame::scan_parquet(&opts.path, scan_args)?; //.with_streaming(true);
    eprintln!(
        "Loaded file {} (took {:?})",
        opts.path.display(),
        start.elapsed()
    );

    let foo = Foo::new(lf)?;

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

const CHUNK_SIZE: u32 = 10_000;

struct Foo {
    lf: LazyFrame,
    total_rows: usize,
    available_rows: Range<usize>, // The rows in big_df
    big_df: Vec<ArrayRef>,
    mini_df: Vec<ArrayRef>,      // A subset of the rows/columns in big_df
    col_stats: Vec<ColumnStats>, // One per column
    idx_width: u16,
}

impl Foo {
    fn new(lf: LazyFrame) -> anyhow::Result<Self> {
        let start = Instant::now();
        let total_rows = lf.clone().select([count()]).collect()?[0]
            .u32()?
            .get(0)
            .unwrap() as usize;
        eprintln!("Counted {total_rows} rows (took {:?})", start.elapsed());
        let col_stats = vec![ColumnStats::default(); lf.schema()?.len()];
        let idx_width = total_rows.ilog10() as u16 + 1;
        Ok(Foo {
            lf,
            total_rows,
            available_rows: 0..0,
            big_df: vec![],
            mini_df: vec![],
            col_stats,
            idx_width,
        })
    }

    fn update_df(
        &mut self,
        start_row: usize,
        start_col: usize,
        term_size: (u16, u16),
    ) -> anyhow::Result<()> {
        let end_row = (start_row + term_size.1 as usize - 2).min(self.total_rows - 1);
        let all_rows_available =
            self.available_rows.contains(&start_row) && self.available_rows.contains(&end_row);
        if !all_rows_available {
            let from = start_row.saturating_sub(1000);
            let mut big_df = self.lf.clone().slice(from as i64, CHUNK_SIZE).collect()?;
            let mut chunk_iter = big_df.as_single_chunk().iter_chunks();
            self.big_df.clear();
            self.big_df.extend(
                chunk_iter
                    .next()
                    .unwrap()
                    .into_arrays()
                    .into_iter()
                    .map(|x| ArrayRef::from(x)),
            );
            assert!(chunk_iter.next().is_none());

            self.available_rows = from..(from + CHUNK_SIZE as usize);
            for ((name, old_stats), col) in self
                .lf
                .schema()?
                .iter_names()
                .zip(self.col_stats.iter_mut())
                .zip(&self.big_df)
            {
                let new_stats = ColumnStats::new(&name, col)?;
                old_stats.merge(new_stats);
            }
        }
        self.mini_df.clear();
        self.mini_df
            .extend(self.big_df.iter().skip(start_col).map(|col| {
                col.slice(
                    start_row - self.available_rows.start,
                    term_size.1 as usize - 2,
                )
            }));
        Ok(())
    }

    fn draw(
        &self,
        stdout: &mut impl Write,
        start_row: usize,
        start_col: usize,
        term_size: (u16, u16),
    ) -> anyhow::Result<()> {
        draw(
            stdout,
            start_row,
            &self.mini_df,
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
        foo.update_df(start_row, start_col, term_size)?;
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
