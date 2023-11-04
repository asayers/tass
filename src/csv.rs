use crate::DataSource;
use anyhow::bail;
use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::Schema;
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
        let mut source = CsvFile {
            fs: FileSlice::new(file.try_clone()?),
            file,
            format: Format::default().with_header(true),
            n_rows: 0,
            schema: Schema::empty().into(),
        };
        match source.format.infer_schema(source.fs.clone(), None) {
            Ok((schema, n_rows)) => {
                source.schema = schema.into();
                source.n_rows = n_rows;
            }
            Err(e) => error!("Couldn't infer schema: {e}"),
        };
        Ok(source)
    }

    fn n_bytes(&self) -> u64 {
        self.fs.clone().seek(SeekFrom::End(0)).unwrap()
    }
}

impl DataSource for CsvFile {
    fn row_count(&mut self) -> anyhow::Result<usize> {
        let n_bytes = self.file.metadata()?.len();
        if n_bytes != self.n_bytes() {
            debug!("File size has changed! ({} -> {})", self.n_bytes(), n_bytes);
            let new_fs = FileSlice::new(self.file.try_clone()?);
            match self.format.infer_schema(self.fs.clone(), None) {
                Ok((schema, n_rows)) => {
                    self.fs = new_fs;
                    self.schema = schema.into();
                    self.n_rows = n_rows;
                    debug!("Counted {n_rows} rows");
                }
                Err(e) => error!("Couldn't infer schema: {e}"),
            };
        }
        Ok(self.n_rows)
    }

    fn fetch_batch(&mut self, offset: usize, len: usize) -> anyhow::Result<RecordBatch> {
        let mut rdr = ReaderBuilder::new(self.schema.clone())
            .with_format(self.format.clone())
            .with_bounds(offset, offset + len)
            .with_batch_size(len)
            .build(self.fs.clone())?;
        match rdr.next() {
            Some(batch) => Ok(batch?),
            None => Ok(RecordBatch::new_empty(self.schema.clone())),
        }
    }

    fn search(&mut self, needle: &str, from: usize, rev: bool) -> anyhow::Result<Option<usize>> {
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
