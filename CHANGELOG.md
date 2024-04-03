## 0.9.0

* Add support for TSV files
* Add a `--format` flag for forcing a certain input format
* Correctly size columns which use the fallback renderer
* Reduce flashing/tearing when redrawing large terminals
* Add _experimental_ support for sorting/filtering (behind a feature flag)

## 0.8.0

* Add fallback rendering for all column types
* Fix a bug where the grid would be empty until you hit a button
* Fix a bug where the terminal would capture mouse events after exit

## 0.7.0

* Click rows to highlight them!
* Better behaviour at the right-hand edge of the terminal when contents are chopped
* Support for bool and binary column types
* Add _experimental_ support for newline-delimited JSON (behind a feature flag)

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
