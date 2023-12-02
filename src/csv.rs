use crate::DataSource;
use anyhow::bail;
use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};
use arrow::record_batch::RecordBatch;
use fileslice::FileSlice;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::Arc;
use tracing::{debug, error};

pub struct CsvFile {
    file: File, // Keep this around for re-generating the fileslice.  TODO: Add FileSlice::refresh()
    fs: FileSlice,
    /// The nth row begins at byte `row_offsets[n]` in `fs`
    row_offsets: Vec<u64>,
    format: Format,
    schema: Arc<Schema>,
}

impl CsvFile {
    pub fn new(file: File) -> anyhow::Result<CsvFile> {
        Ok(CsvFile {
            fs: FileSlice::new(file.try_clone()?).slice(0, 0),
            file,
            format: Format::default().with_header(false),
            row_offsets: vec![],
            schema: Schema::empty().into(),
        })
    }

    fn n_bytes(&self) -> u64 {
        self.fs.clone().seek(SeekFrom::End(0)).unwrap()
    }
}

impl DataSource for CsvFile {
    fn check_for_new_rows(&mut self) -> anyhow::Result<bool> {
        let n_bytes_then = self.n_bytes();
        let n_bytes_now = self.file.metadata()?.len();
        if n_bytes_now == n_bytes_then {
            return Ok(false);
        }

        debug!("File size has changed! ({n_bytes_then} -> {n_bytes_now})");
        let new_fs = FileSlice::new(self.file.try_clone()?);

        if self.row_offsets.is_empty() {
            // Initial schema inference should read the header
            let format = self.format.clone().with_header(true);
            let schema = match format.infer_schema(new_fs.clone(), None) {
                Ok((x, _)) => x,
                Err(e) => {
                    error!("Couldn't infer schema: {e}");
                    return Ok(false);
                }
            };
            let mut bldr = SchemaBuilder::new();
            for field in schema.fields() {
                let field = match field.data_type() {
                    DataType::Timestamp(_, _) => {
                        let f: &Field = &field;
                        f.clone().with_data_type(DataType::Utf8).into()
                    }
                    _ => field.clone(),
                };
                bldr.push(field);
            }
            self.schema = bldr.finish().into();

            // TODO: Optimize
            // We skip one line (the header) since this is the initial read
            let mut line_start = 0;
            let lines = BufReader::new(new_fs.slice(line_start, n_bytes_now)).lines();
            for line in lines {
                line_start += line?.len() as u64 + 1;
                self.row_offsets.push(line_start);
            }

            self.fs = new_fs;
            debug!("Read {} rows", self.row_count());
            debug!("First row starts at byte {}", self.row_offsets[0]);
            debug!(
                "Final row starts at byte {}",
                self.row_offsets.last().unwrap(),
            );
            Ok(true)
        } else {
            // TODO: Confirm that schemas match
            // TODO: Optimize
            let n_rows_then = self.row_count();
            let mut line_start = *self.row_offsets.last().unwrap();
            let new_lines = BufReader::new(new_fs.slice(line_start, n_bytes_now)).lines();
            for line in new_lines {
                line_start += line?.len() as u64 + 1;
                self.row_offsets.push(line_start);
            }
            self.fs = new_fs;
            debug!("Added {} new rows", self.row_count() - n_rows_then);
            Ok(true)
        }
    }

    fn row_count(&self) -> usize {
        self.row_offsets.len().saturating_sub(1)
    }

    fn fetch_batch(&self, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
        let row_to_byte = |row: usize| -> u64 {
            self.row_offsets
                .get(row)
                .copied()
                .unwrap_or_else(|| self.n_bytes())
        };
        let byte_start = row_to_byte(offset);
        let byte_end = row_to_byte(offset + len + 1);
        let slice = self.fs.slice(byte_start, byte_end);
        let mut rdr = ReaderBuilder::new(self.schema.clone())
            .with_format(self.format.clone())
            .with_bounds(0, len)
            .with_batch_size(len)
            .build(slice)?;
        // debug!("{:?}", self.schema);
        match rdr.next() {
            Some(batch) => Ok(batch?),
            None => Ok(RecordBatch::new_empty(self.schema.clone())),
        }
    }

    fn search(&self, needle: &str, from: usize, rev: bool) -> anyhow::Result<Option<usize>> {
        if rev {
            bail!("Reverse-searching CSV not supported yet");
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
