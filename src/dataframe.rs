use crate::get_index;
use anyhow::{anyhow, Context};
use ndarray::prelude::*;
use std::cmp::min;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::time::Duration;

pub struct DataFrame {
    headers: Vec<String>,
    rdr: csv::Reader<File>,
    search_file: File,
}

impl DataFrame {
    pub fn new(path: &Path) -> anyhow::Result<DataFrame> {
        let mut newlines = get_index();
        const MIN_LINES: usize = 2;
        let n = newlines.len();
        if n < MIN_LINES {
            let mut sleep = Duration::from_millis(1);
            while newlines.len() < MIN_LINES {
                newlines.update().context("Updating newlines")?;
                std::thread::sleep(sleep);
                if sleep < Duration::from_millis(500) {
                    sleep *= 2; // Start with an exponential backoff
                }
                if sleep > Duration::from_millis(500) {
                    // Looks like we'll be waiting a while...
                    eprintln!(
                        "Waiting for more data... (saw {} lines but need at least {})",
                        n, MIN_LINES
                    );
                    sleep = Duration::from_millis(500);
                }
            }
        }

        let file = File::open(path)
            .context(path.display().to_string())
            .context("Opening file again to read actual data")?;
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .trim(csv::Trim::All)
            .from_reader(file);
        let headers = rdr
            .headers()?
            .into_iter()
            .map(|x| x.to_owned())
            .collect::<Vec<_>>();
        Ok(DataFrame {
            headers,
            rdr,
            search_file: File::open(path)?,
        })
    }

    pub fn get_headers(&self) -> impl Iterator<Item = &str> + '_ {
        self.headers.iter().map(|x| x.as_str())
    }

    pub fn get_line(&mut self, line: usize) -> anyhow::Result<csv::StringRecord> {
        let pos = get_index().line2pos(line);
        self.rdr.seek(pos)?;
        let record = self
            .rdr
            .records()
            .next()
            .ok_or_else(|| anyhow!("no records"))?;
        Ok(record?)
    }

    pub fn get_data(
        &mut self,
        start_line: usize,
        end_line: usize,
    ) -> anyhow::Result<Array2<String>> {
        let pos = get_index().line2pos(start_line);
        self.rdr.seek(pos)?;

        let n_rows = end_line - start_line;
        let n_cols = self.headers.len();

        let mut vals = Vec::with_capacity(n_cols * n_rows);
        for row in self.rdr.records().take(n_rows) {
            for val in &row? {
                vals.push(val.to_owned());
            }
        }

        Ok(Array2::from_shape_vec((n_rows, n_cols), vals)?)
    }

    pub fn len(&self) -> usize {
        get_index().len()
    }

    pub fn search(&mut self, start_line: usize, pattern: &str) -> anyhow::Result<Option<usize>> {
        let index = get_index();
        let max_line = index.len() - 2;
        let add = |start_line: usize, x: usize| min(max_line, start_line.saturating_add(x));
        let x = index.line2range(start_line).start;
        self.search_file.seek(SeekFrom::Start(x))?;
        let matcher = grep_regex::RegexMatcher::new(pattern)?;
        let mut ret = None;
        let sink = grep_searcher::sinks::UTF8(|line, _| {
            ret = Some(add(start_line - 1, line as usize - 1));
            Ok(false)
        });
        grep_searcher::Searcher::new().search_file(&matcher, &self.search_file, sink)?;
        Ok(ret)
    }
}
