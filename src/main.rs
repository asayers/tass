mod draw;
mod stats;

use crate::draw::*;
use crate::stats::*;
use anyhow::Context;
use bpaf::{Bpaf, Parser};
use crossterm::*;
use polars::prelude::*;
use std::io::Write;
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
    let scan_args = ScanArgsParquet::default();
    let mut lf = LazyFrame::scan_parquet(&opts.path, scan_args)?;
    eprintln!(
        "Loaded file {} (took {:?})",
        opts.path.display(),
        start.elapsed()
    );

    let start = Instant::now();
    let total_rows = lf.clone().select([count()]).collect()?[0]
        .u32()?
        .get(0)
        .unwrap() as usize;
    eprintln!("Counted {total_rows} rows (took {:?})", start.elapsed());

    // let mut n = 0;
    // for null_count in lf.clone().null_count().collect()?.get_columns() {
    //     let n_nulls = null_count.u32()?.get(0).unwrap() as usize;
    //     if n_nulls == total_rows {
    //         lf = lf.drop_columns([null_count.name()]);
    //         n += 1;
    //     }
    // }
    // eprintln!("Hid {n} empty columns");

    let col_stats = vec![ColumnStats::default(); lf.schema()?.len()];
    // lf       .schema()?
    //        .iter_fields()
    //        .map(|x| ColumnStats::new(&lf, &x.name, x.dtype))
    //        .collect::<anyhow::Result<Vec<ColumnStats>>>()?;

    // The width of the widest value in each column, when formatted (including the header)
    let col_widths = lf
        .schema()?
        .iter_fields()
        .zip(&col_stats)
        .map(|(field, stats)| (field.name.len() as u16).max(stats.max_len))
        .collect::<Vec<u16>>();

    // let lf = lf.with_row_count("__parquess_idx__", None);

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    // Set up terminal
    terminal::enable_raw_mode().context("entering raw mode")?;
    stdout
        .queue(terminal::EnterAlternateScreen)?
        .queue(terminal::DisableLineWrap)?
        .flush()?;

    // Store the result so the cleanup happens even if there's an error
    let result = runloop(&mut stdout, lf, total_rows, col_widths, col_stats);

    // Clean up terminal
    stdout
        .queue(terminal::EnableLineWrap)?
        .queue(terminal::LeaveAlternateScreen)?
        .flush()?;
    terminal::disable_raw_mode()?;
    result
}

const CHUNK_SIZE: u32 = 10_000;

fn runloop(
    stdout: &mut impl Write,
    lf: LazyFrame,
    max_row: usize,
    col_widths: Vec<u16>,
    col_stats: Vec<ColumnStats>,
) -> anyhow::Result<()> {
    let mut term_size = terminal::size()?;
    let mut big_df = lf.clone().slice(0, CHUNK_SIZE).collect()?;
    let mut mini_df;
    let idx_width = max_row.ilog10() as u16 + 1;
    let mut start_col: usize = 0;
    let mut start_row: usize = 0;
    macro_rules! update_df {
        () => {{
            let first_available = big_df[0].idx()?.get(0).unwrap_or(0) as usize;
            let last_available = big_df[0].idx()?.last().unwrap_or(0) as usize;
            let available = first_available..=last_available;
            let end_row = (start_row + term_size.1 as usize - 2).min(max_row - 1);
            let all_rows_available = available.contains(&start_row) && available.contains(&end_row);
            if !all_rows_available {
                big_df = lf
                    .clone()
                    .slice(start_row.saturating_sub(1000) as i64, CHUNK_SIZE)
                    .collect()?;
            }
            mini_df = big_df
                .filter(&big_df[0].idx()?.gt_eq(start_row))?
                .head(Some(term_size.1 as usize - 2));
        }};
    }
    update_df!();

    loop {
        draw(
            stdout,
            &mini_df.get_columns()[0],
            &mini_df.select_by_range(1 + start_col..)?,
            term_size,
            idx_width,
            &col_widths[start_col..],
            &col_stats[start_col..],
        )?;
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
                        start_row = (start_row + 1).min(max_row - 1)
                    }
                    event::KeyCode::Up | event::KeyCode::Char('k') => {
                        start_row = start_row.saturating_sub(1)
                    }
                    event::KeyCode::End | event::KeyCode::Char('G') => start_row = max_row - 1,
                    event::KeyCode::Home | event::KeyCode::Char('g') => start_row = 0,
                    event::KeyCode::PageUp => {
                        start_row = start_row.saturating_sub(term_size.1 as usize - 2)
                    }
                    event::KeyCode::PageDown => {
                        start_row = (start_row + term_size.1 as usize - 2).min(max_row - 1)
                    }
                    _ => (),
                },
                event::Event::Resize(cols, rows) => term_size = (cols, rows),
                _ => (),
            }
            update_df!();
        }
    }
}
