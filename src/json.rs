use crate::DataSource;
use anyhow::bail;
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};
use arrow::json::reader::infer_json_schema;
use arrow::json::ReaderBuilder;
use arrow::record_batch::RecordBatch;
use fileslice::FileSlice;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

pub struct JsonFile {
    file: File, // Keep this around for re-generating the fileslice.  TODO: Add FileSlice::refresh()
    fs: FileSlice,
    /// The nth row begins at byte `row_offsets[n]` in `fs`
    row_offsets: Vec<u64>,
    schema: Arc<Schema>,
}

impl JsonFile {
    pub fn new(file: File) -> anyhow::Result<JsonFile> {
        warn!("JSON support is experimental");
        Ok(JsonFile {
            fs: FileSlice::new(file.try_clone()?).slice(0, 0),
            file,
            row_offsets: vec![],
            schema: Schema::empty().into(),
        })
    }

    fn n_bytes(&self) -> u64 {
        self.fs.clone().seek(SeekFrom::End(0)).unwrap()
    }

    // TODO: Optimize (memchr + mmap?)
    fn add_new_lines(&mut self) -> anyhow::Result<usize> {
        let n_rows_then = self.row_count();
        let mut line_start = self.row_offsets.last().copied().unwrap_or(0);
        let new_lines = BufReader::new(self.fs.slice(line_start, self.n_bytes())).lines();
        let start = Instant::now();
        for line in new_lines {
            line_start += line?.len() as u64 + 1;
            self.row_offsets.push(line_start);
            if start.elapsed() > Duration::from_millis(10) {
                break;
            }
        }
        Ok(self.row_count() - n_rows_then)
    }

    /// Merge `schema` into `self.schema`
    fn merge_schema(&mut self, schema: Schema) {
        let mut bldr = SchemaBuilder::new();
        for old in self.schema.fields() {
            let Some((_, new)) = schema.fields().find(old.name()) else {
                bldr.push(old.clone());
                continue;
            };
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
        for new in schema.fields() {
            if self.schema.fields().find(new.name()).is_none() {
                let new = match new.data_type() {
                    DataType::Timestamp(_, _) => {
                        Field::clone(new).with_data_type(DataType::Utf8).into()
                    }
                    _ => new.clone(),
                };
                bldr.push(new);
            }
        }
        self.schema = bldr.finish().into();
        debug!("Merged new schema into the existing one");
    }
}

impl DataSource for JsonFile {
    fn check_for_new_rows(&mut self) -> anyhow::Result<usize> {
        let n_bytes_then = self.n_bytes();
        let n_bytes_now = self.file.metadata()?.len();
        if n_bytes_now == n_bytes_then {
            return Ok(0);
        }
        debug!("File size has changed! ({n_bytes_then} -> {n_bytes_now})");
        self.fs = FileSlice::new(self.file.try_clone()?);

        let n = self.add_new_lines()?;
        debug!("Added {n} new rows");
        if n == 0 {
            error!("Caught up with the EOF");
        } else {
            self.fs = self.fs.slice(0, *self.row_offsets.last().unwrap());
        }

        Ok(n)
    }

    fn row_count(&self) -> usize {
        self.row_offsets.len().saturating_sub(1)
    }

    fn fetch_batch(&mut self, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
        debug!(offset, len, "Fetching a batch");
        let byte_start = self
            .row_offsets
            .get(offset)
            .copied()
            .unwrap_or_else(|| self.n_bytes());
        let slice = self.fs.slice(byte_start, self.n_bytes());
        debug!(byte_start, "Sliced the file");

        let (schema, _) = infer_json_schema(BufReader::new(slice.clone()), None)?;
        self.merge_schema(schema);

        let mut rdr = ReaderBuilder::new(self.schema.clone())
            .with_batch_size(len)
            .build(BufReader::new(slice))?;
        let batch = match rdr.next() {
            Some(batch) => batch?,
            None => RecordBatch::new_empty(self.schema.clone()),
        };
        debug!(len = batch.num_rows(), "Loaded a record batch");

        Ok(batch)
    }

    fn search(&self, needle: &str, from: usize, rev: bool) -> anyhow::Result<Option<usize>> {
        if rev {
            bail!("Reverse-searching JSON not supported yet");
        }
        // FIXME: Not all newlines are new rows in CSV
        for (row, txt) in BufReader::new(self.fs.clone())
            .lines()
            .enumerate()
            .skip(from + 1 /* header */ + 1 /* current_row */)
        {
            let txt = txt?;
            if memchr::memmem::find(txt.as_bytes(), needle.as_bytes()).is_some() {
                return Ok(Some(row - 1));
            }
        }
        Ok(None)
    }
}
