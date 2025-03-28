use super::DataSource;
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};
use arrow::json::ReaderBuilder;
use arrow::json::reader::infer_json_schema;
use arrow::record_batch::RecordBatch;
use fileslice::FileSlice;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, info_span, warn};

pub struct JsonFile {
    fs: FileSlice,
    /// The nth row begins at byte `row_offsets[n]` in `fs`
    row_offsets: Vec<u64>,
    schema: Arc<Schema>,
}

impl JsonFile {
    pub fn new(file: File) -> anyhow::Result<JsonFile> {
        warn!("JSON support is experimental");
        Ok(JsonFile {
            fs: FileSlice::new(file.try_clone()?).slice(0..0),
            row_offsets: vec![],
            schema: Schema::empty().into(),
        })
    }

    // TODO: Optimize (memchr + mmap?)
    fn add_new_lines(&mut self) -> anyhow::Result<usize> {
        let n_rows_then = self.row_count();
        let mut line_start = self.row_offsets.last().copied().unwrap_or(0);
        let new_lines = BufReader::new(self.fs.slice(line_start..)).lines();
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
            let _g = info_span!("", name).entered();
            let nullable = old.is_nullable()
                || new.is_nullable()
                || old.data_type() == &DataType::Null
                || new.data_type() == &DataType::Null;
            let dtype = match (old.data_type(), new.data_type()) {
                (_, DataType::Timestamp(_, _)) => DataType::Utf8,
                (x, DataType::Null) => x.clone(),
                (DataType::Null, y) => y.clone(),
                (x, y) if x == y => x.clone(),
                (x, y) if stringlike(x) && stringlike(y) => {
                    info!("{x} & {y}: Casting to Utf8");
                    DataType::Utf8
                }
                (x, y) => {
                    error!("Can't unify {x} & {y}");
                    warn!("Dropping column");
                    continue;
                }
            };
            let merged = Field::new(name, dtype, nullable);
            if &merged != old.as_ref() {
                info!(
                    "Updated schema: {} -> {}",
                    old.data_type(),
                    merged.data_type(),
                );
            }
            bldr.push(merged);
        }
        for new in schema
            .fields()
            .iter()
            .filter(|x| self.schema.fields().find(x.name()).is_none())
        {
            let _g = info_span!("", name = new.name()).entered();
            let new = match new.data_type() {
                DataType::Timestamp(_, _) => {
                    Field::clone(new).with_data_type(DataType::Utf8).into()
                }
                _ => new.clone(),
            };
            info!("New field: {}", new.data_type());
            bldr.push(new);
        }
        self.schema = bldr.finish().into();
        debug!("Merged new schema into the existing one");
    }
}

fn stringlike(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Timestamp(..)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(..)
            | DataType::Time64(..)
            | DataType::Duration(..)
            | DataType::Interval(..)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    )
}

impl DataSource for JsonFile {
    fn check_for_new_rows(&mut self) -> anyhow::Result<usize> {
        let n_bytes_then = self.fs.end_pos();
        self.fs.expand();
        let n_bytes_now = self.fs.end_pos();
        if n_bytes_now == n_bytes_then {
            return Ok(0);
        }
        debug!("File size has changed! ({n_bytes_then} -> {n_bytes_now})");

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

        let (schema, _n_rows) = infer_json_schema(BufReader::new(slice.clone()), None)?;
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

    fn search(&self, needle: &str) -> anyhow::Result<Vec<usize>> {
        let mut matches = vec![];
        for (row, txt) in BufReader::new(self.fs.clone()).lines().enumerate() {
            let txt = txt?;
            if txt.contains(needle) {
                matches.push(row);
            }
        }
        Ok(matches)
    }
}
