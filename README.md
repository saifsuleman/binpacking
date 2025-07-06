# BinPacking CSV Splitter

A command-line tool written in Go to efficiently **split a large CSV file** into multiple smaller files based on the size of each line. This tool uses **greedy bin packing** to balance the total size of data in each output file, ensuring that each output is roughly equal in total content size rather than just line count.

---

## Features

* **Greedy bin packing** algorithm based on CSV line size.
* **Multithreaded streaming write** to output files.
* Efficient **two-pass read**: one for metadata gathering, one for writing.
* Handles extremely large CSVs by optimizing memory usage and processing.

---

## Use Cases

* Splitting large CSVs into manageable files for parallel processing.
* Load balancing large data imports across distributed systems.
* Preparing data for batch processing, sharding, or ingestion pipelines.

---

## Assumptions

* The input CSV contains a `size` column in the **third column (index 2)** which indicates the size (in bytes) of each row.
* The CSV has a **header line** that is preserved across all output files.

---

## Installation

1. Make sure you have Go installed (`go version` should be >= 1.18).
2. Clone the repository:

```bash
git clone https://github.com/yourusername/binpacking-csv.git
cd binpacking-csv
```

3. Build the binary:

```bash
go build -o binpacking
```

---

## Usage

The CLI has two commands:

### 1. `split`

Split a CSV into multiple files, distributing rows such that each file has a similar **total row size**, not row count.

```bash
./binpacking split <input_csv> <buckets> <output_prefix>
```

* `<input_csv>`: Path to the input CSV file.
* `<buckets>`: Number of output files to create.
* `<output_prefix>`: Prefix for output filenames. Files will be named like `<output_prefix>1.csv`, `<output_prefix>2.csv`, etc.

**Example:**

```bash
./binpacking split data.csv 4 output/data_
```

This will create `data_1.csv` to `data_4.csv` with balanced total size across the files.

---

### 2. `inspect`

Prints total number of lines and cumulative size of the input CSV, using the value in the third column.

```bash
./binpacking inspect <input_csv>
```

**Example:**

```bash
./binpacking inspect data.csv
```

Outputs something like:

```
Processed 1,000,000 lines...
Total lines: 1,234,567, Total size: 489MB
```

---
## Example CSV Format

```csv
id,name,size
1,Alice,1234
2,Bob,4200
3,Charlie,512
...
```

