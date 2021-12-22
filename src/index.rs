use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Range;
use std::path::Path;

/// Records the locations of all newlines in a file.
pub struct Index {
    offset: u64,
    newlines: Vec<u64>,
    file: Option<BufReader<File>>,
    pub watch_for_updates: bool,
    up_to_date: bool,
}

impl Index {
    pub fn from_file(path: &Path) -> anyhow::Result<Index> {
        let mut ret = Index {
            offset: 0,
            file: Some(BufReader::new(File::open(path)?)),
            newlines: vec![],
            watch_for_updates: true,
            up_to_date: false,
        };
        ret.update()?;
        Ok(ret)
    }

    pub fn no_file() -> Index {
        Index {
            offset: 0,
            file: None,
            newlines: vec![],
            watch_for_updates: true,
            up_to_date: false,
        }
    }

    pub fn stop_watching(&mut self) {
        self.up_to_date = true;
        self.watch_for_updates = false;
    }

    pub fn up_to_date(&self) -> bool {
        self.up_to_date
    }

    /// `len` is the length of the line _without_ newline.
    pub fn push_line(&mut self, len: u64) {
        self.newlines.push(self.offset + len);
        self.offset += len + 1;
    }

    /// Reads the file, starting at EOF the last time this function was
    /// called, up to the current EOF, adding line-break offsets to `newlines`.
    pub fn update(&mut self) -> anyhow::Result<()> {
        if !self.watch_for_updates {
            return Ok(());
        }
        if self.file.is_none() {
            return Ok(());
        }
        let n_lines_start = self.len();
        loop {
            if self.len() - n_lines_start > 1_000_000 {
                self.up_to_date = false;
                return Ok(());
            }
            let buf = self.file.as_mut().unwrap().fill_buf()?;
            if buf.is_empty() {
                self.up_to_date = true;
                return Ok(());
            }
            if let Some(x) = memchr::memchr(b'\n', buf) {
                self.newlines.push(self.offset + x as u64);
                self.offset += x as u64 + 1;
                self.file.as_mut().unwrap().consume(x + 1);
            } else {
                let x = buf.len();
                self.offset += x as u64;
                self.file.as_mut().unwrap().consume(x);
            }
        }
    }

    /// Gives a byte-range which doesn't include the newline
    pub fn line2range(&self, line: usize) -> Option<Range<u64>> {
        let lhs = if line == 0 {
            0
        } else {
            *self.newlines.get(line - 1)? + 1
        };
        let rhs = *self.newlines.get(line)?;
        Some(lhs..rhs)
    }

    pub fn line2pos(&self, mut line: usize) -> Option<csv::Position> {
        line += 1;
        let mut pos = csv::Position::new();
        pos.set_line(line as u64)
            .set_byte(self.line2range(line)?.start)
            .set_record(0);
        Some(pos)
    }

    pub fn len(&self) -> usize {
        self.newlines.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor, Write};
    use tempfile::*;

    #[test]
    fn test() {
        let mut f = NamedTempFile::new().unwrap();
        let s = b"foo,bar\n1,2\n3,4\n";
        f.write_all(s).unwrap();
        let lines = Index::from_file(f.path()).unwrap();
        assert_eq!(lines.len(), 3);
        // line2range never includes the newline char, hence the non-contiguous
        // ranges
        assert_eq!(lines.line2range(0), Some(0..7));
        assert_eq!(lines.line2range(1), Some(8..11));
        assert_eq!(lines.line2range(2), Some(12..15));
        assert_eq!(s.len(), 16);
    }

    #[test]
    fn test_stream() {
        let mut f = NamedTempFile::new().unwrap();
        let s = b"foo,bar\n1,2\n3,4\n";
        f.write_all(s).unwrap();
        let idx1 = Index::from_file(f.path()).unwrap();

        let s = b"foo,bar\n1,2\n3,4\n".to_vec();
        let mut idx2 = Index::no_file();
        for l in BufReader::new(Cursor::new(s)).lines() {
            idx2.push_line(l.unwrap().len() as u64);
        }

        assert_eq!(idx1.newlines, idx2.newlines);
    }
}
