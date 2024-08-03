use super::DataSource;
use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};
use arrow::record_batch::RecordBatch;
use fileslice::FileSlice;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

pub struct CsvFile {
    fs: FileSlice,
    /// The nth row begins at byte `row_offsets[n]` in `fs`
    row_offsets: Vec<u64>,
    format: Format,
    schema: Arc<Schema>,
}

impl CsvFile {
    pub fn new(file: File, delimiter: u8) -> anyhow::Result<CsvFile> {
        Ok(CsvFile {
            fs: FileSlice::new(file.try_clone()?).slice(0..0),
            format: Format::default()
                .with_header(false)
                .with_delimiter(delimiter),
            row_offsets: vec![],
            schema: Schema::empty().into(),
        })
    }

    /// Initial schema inference just reads the header row to get the
    /// names of the fields.  The inferred datatype of all columns will
    /// just be "null" at this point
    fn read_header(&mut self) -> anyhow::Result<()> {
        let format = self.format.clone().with_header(true);
        let (schema, n_rows) = format.infer_schema(self.fs.clone(), Some(0))?;
        assert_eq!(n_rows, 0);
        self.schema = schema.into();
        for f in self.schema.fields() {
            assert_eq!(f.data_type(), &DataType::Null);
            info!("Read header {}", f.name());
        }
        Ok(())
    }

    // TODO: Optimize (memchr + mmap?)
    // FIXME: Not all newlines are new rows in CSV
    fn add_new_lines(&mut self) -> anyhow::Result<usize> {
        let n_rows_then = self.row_count();
        let mut line_start = self.row_offsets.last().copied().unwrap_or(0);

        let mut new_bytes = BufReader::new(self.fs.slice(line_start..));
        let start = Instant::now();
        let mut line = Vec::new();

        loop {
            new_bytes.read_until(b'\n', &mut line)?;

            // If we reached EOF rather than a newline, ensure we don't record that as a row offset
            if line.last().map_or(true, |b| *b != b'\n') {
                break;
            }
            line_start += line.len() as u64;
            self.row_offsets.push(line_start);
            line.clear();

            if start.elapsed() > Duration::from_millis(10) {
                break;
            }
        }
        Ok(self.row_count() - n_rows_then)
    }

    /// Merge `schema` into `self.schema`
    fn merge_schema(&mut self, schema: Schema) {
        let mut bldr = SchemaBuilder::new();
        for (old, new) in self.schema.fields().iter().zip(schema.fields()) {
            let name = old.name();
            let nullable = old.is_nullable()
                || new.is_nullable()
                || old.data_type() == &DataType::Null
                || new.data_type() == &DataType::Null;
            let dtype = match (old.data_type(), new.data_type()) {
                (_, DataType::Timestamp(_, _)) => DataType::Utf8,
                (x, DataType::Null) => x.clone(),
                (DataType::Null, y) => y.clone(),
                (x, y) if x == y => x.clone(),
                _ => DataType::Utf8,
            };
            let merged = Field::new(name, dtype, nullable);
            if &merged != old.as_ref() {
                info!(
                    "Updated schema for {}: {} -> {}",
                    old.name(),
                    old.data_type(),
                    merged.data_type(),
                );
            }
            bldr.push(merged);
        }
        self.schema = bldr.finish().into();
        debug!("Merged new schema into the existing one");
    }
}

impl DataSource for CsvFile {
    fn check_for_new_rows(&mut self) -> anyhow::Result<usize> {
        let n_bytes_then = self.fs.end_pos();
        self.fs.expand();
        let n_bytes_now = self.fs.end_pos();
        if n_bytes_now == n_bytes_then {
            return Ok(0);
        }
        debug!("File size has changed! ({n_bytes_then} -> {n_bytes_now})");

        if self.schema.fields().is_empty() {
            match self.read_header() {
                Ok(()) => (),
                Err(_) => return Ok(0),
            }
        }

        let n = self.add_new_lines()?;
        debug!("Added {n} new rows");
        if n == 0 {
            error!("Caught up with the EOF");
        } else {
            let x = *self.row_offsets.last().unwrap();
            self.fs = self.fs.slice(..x);
        }

        Ok(n)
    }

    fn row_count(&self) -> usize {
        self.row_offsets.len().saturating_sub(1)
    }

    fn fetch_batch(&mut self, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
        debug!(offset, len, "Fetching a batch");
        let row_to_byte = |row: usize| -> u64 {
            self.row_offsets
                .get(row)
                .copied()
                .unwrap_or_else(|| self.fs.end_pos())
        };
        let byte_start = row_to_byte(offset);
        let byte_end = row_to_byte(offset + len + 1);
        let slice = self.fs.slice(byte_start..byte_end);
        debug!(byte_start, byte_end, "Sliced the file");

        let (schema, _n_rows) = self.format.infer_schema(slice.clone(), None)?;
        self.merge_schema(schema);

        let mut rdr = ReaderBuilder::new(self.schema.clone())
            .with_format(self.format.clone())
            .with_bounds(0, len)
            .with_batch_size(len)
            .build(slice)?;
        let batch = match rdr.next() {
            Some(batch) => batch?,
            None => RecordBatch::new_empty(self.schema.clone()),
        };
        debug!(len = batch.num_rows(), "Loaded a record batch");

        Ok(batch)
    }

    // FIXME: Not all newlines are new rows in CSV
    fn search(&self, needle: &str) -> anyhow::Result<Vec<usize>> {
        let mut matches = vec![];
        for (row, txt) in BufReader::new(self.fs.clone()).lines().skip(1).enumerate() {
            let txt = txt?;
            if txt.contains(needle) {
                matches.push(row);
            }
        }
        Ok(matches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // This tests the situation in which `tass` catches up with stdin, running out of bytes
    // in the middle of a line.
    #[test]
    fn handles_unfinished_line() -> anyhow::Result<()> {
        let data = "\
a,b,c,d
1,2,3,4
5,6,7,8
9,👻,10,11
12,13,14,15";

        let halfway_through_codepoint = data.find("👻").unwrap() + 1;

        let mut tmp = tempfile::NamedTempFile::new()?;
        tmp.write_all(&data.as_bytes()[..halfway_through_codepoint])?;

        let mut source = CsvFile::new(File::open(tmp.path())?, b',')?;
        source.check_for_new_rows()?;

        tmp.write_all(&data.as_bytes()[halfway_through_codepoint..])?;
        source.check_for_new_rows()?;

        assert_eq!(source.row_offsets, [8, 16, 24, 37]);

        Ok(())
    }
}
