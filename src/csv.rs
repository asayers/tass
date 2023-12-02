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
    n_rows: usize,
    format: Format,
    schema: Arc<Schema>,
}

impl CsvFile {
    pub fn new(file: File) -> anyhow::Result<CsvFile> {
        Ok(CsvFile {
            fs: FileSlice::new(file.try_clone()?).slice(0, 0),
            file,
            format: Format::default().with_header(true),
            n_rows: 0,
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

        if self.n_rows == 0 {
            let (schema, n_rows) = match self.format.infer_schema(new_fs.clone(), None) {
                Ok(x) => x,
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
            self.n_rows = n_rows;
            self.fs = new_fs;
            debug!("Read {n_rows} rows");
            Ok(true)
        } else {
            // TODO: Confirm that schemas match
            let new_bytes = BufReader::new(new_fs.slice(n_bytes_then, n_bytes_now));
            let n_new_rows = new_bytes.lines().count();
            debug!("Added {n_new_rows} new rows");
            self.n_rows += n_new_rows;
            self.fs = new_fs;
            Ok(true)
        }
    }

    fn row_count(&self) -> usize {
        self.n_rows
    }

    fn fetch_batch(&self, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
        let mut rdr = ReaderBuilder::new(self.schema.clone())
            .with_format(self.format.clone())
            .with_bounds(offset, offset + len)
            .with_batch_size(len)
            .build(self.fs.clone())?;
        debug!("{:?}", self.schema);
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
