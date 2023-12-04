## 0.6.0

* Support reading from parquet files
* Performance improvements across the board
* Prettify the header and footer rows
* Show negative values in red and zeroes in grey
* Add a `--precision` flag for controlling the formatting of floating-point columns
* Columns widths are more stable
* Internally, tass now represents data as using Arrow arrays

## 0.5.0

* Start the UI immediately, scan newline off-main-thread
* Stop reading stdin after ctrl-C
* Update the UI immdiately when a followed file is updated

## 0.4.0

* Estimate whether data is numerical, textual, or categorical
* Colourise categorical data
* Right-align numerical data
* Add the `--color-scheme` flag

## 0.3.0

* Fixed a longstanding bug where the final row would be missing

## 0.2.0

* Search for line by regex
* Minimise columns
* Handle streaming from stdin

## 0.1.0

* Build the newlines-index
* Display a grid
