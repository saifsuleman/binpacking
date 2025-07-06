package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

// Due to extremely large file size, we are going to load the line metas separately in memory to perform greedy binpacking sorting, and then later based on this linemeta we will do another pass to stream our input and then stream to an output based on sorted line metas

type LineMeta struct {
	LineNumber int
	Size 			 int64
}

type FileBucket struct {
	TotalSize int64
	LineNums 	map[int]struct{}
}

var rootCmd = &cobra.Command{
	Use: 	"binpacking",
	Short: "Split a large CSV file into smaller files based on line size",
}

var splitCmd = &cobra.Command{
	Use:   "split <input_csv> <buckets> <output_prefix>",
	Short: "Split the input CSV file into smaller files",
	Args:  cobra.ExactArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		input := args[0]
		bucketsN, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println("Error: buckets must be an integer")
			os.Exit(1)
		}
		prefix := args[2]
		metas := scan(input)
		buckets := binpack(metas, bucketsN)
		write(input, prefix, buckets)
		fmt.Printf("Split %s into %d files with prefix %s\n", input, bucketsN, prefix)
	},
}

var inspectCmd = &cobra.Command{
	Use: "inspect <input_csv>",
	Short: "Print the number of entries and total size of the input CSV file",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		input := args[0]
		f, err := os.Open(input)
		if err != nil {
			fmt.Println("Error opening file:", err)
			os.Exit(1)
		}
		defer f.Close()

		r := csv.NewReader(bufio.NewReader(f))
		lineCount := 0
		totalSize := int64(0)

		r.Read()

		for {
			record, err := r.Read()
			if err != nil {
				break
			}
			lineCount++
			size, err := strconv.Atoi(record[2])
			if err != nil {
				fmt.Printf("Error parsing size for line %d: %v\n", lineCount, err)
				continue
			}
			totalSize += int64(size)

			if lineCount % 1000000 == 0 {
				fmt.Printf("Processed %d lines...\n", lineCount)
			}
		}

		fmt.Printf("Total lines: %d, Total size: %sMB\n", lineCount, FormatNumber(totalSize / (1024 * 1024)))
	},
}

func main() {
	rootCmd.AddCommand(splitCmd)
	rootCmd.AddCommand(inspectCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func scan(filename string) []LineMeta {
	start := time.Now()
	fmt.Println("[meta scan] scanning file for line sizes...")
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	metas := []LineMeta{}
	line := 0

	// Skip header
	_, err = r.Read()
	if err != nil {
		panic(err)
	}

	line++

	for {
		record, err := r.Read()
		if err != nil {
			break
		}
		size, _ := strconv.ParseInt(record[2], 10, 64)
		metas = append(metas, LineMeta{LineNumber: line, Size: size})
		line++

		if line % 1000000 == 0 {
			fmt.Printf("[meta scan] %d lines...\n", line)
		}
	}

	end := time.Now()
	fmt.Printf("[meta scan] scan finished %d lines in %s\n\n", len(metas), end.Sub(start))

	return metas
}

func binpack(metas []LineMeta, bucketsN int) []FileBucket {
	start := time.Now()
	fmt.Println("[binpack] sorting line metas by size...")
	sort.Slice(metas, func (i, j int) bool {
		return metas[i].Size > metas[j].Size
	})

	buckets := make([]FileBucket, bucketsN)
	for i := range buckets {
		buckets[i].LineNums = make(map[int]struct{})
	}

	for _, meta := range metas {
		minIndex := 0
		for i := 1; i < bucketsN; i++ {
			if buckets[i].TotalSize < buckets[minIndex].TotalSize {
				minIndex = i
			}
		}
		buckets[minIndex].TotalSize += meta.Size
		buckets[minIndex].LineNums[meta.LineNumber] = struct{}{} // go does not have a Set data structure ;(
	}
	end := time.Now()
	fmt.Printf("[binpack] binpacking finished in %s\n", end.Sub(start))
	for i, bucket := range buckets {
		fmt.Printf("Bucket %d: Total Size = %d, Lines = %d\n", i+1, bucket.TotalSize, len(bucket.LineNums))
	}
	return buckets
}

type RecordData struct {
	record []string
	lineNum int
}

func writerRoutine(ch <- chan RecordData, w *csv.Writer, done chan<- struct{}) {
	for rec := range ch {
		w.Write(rec.record)
	}
	w.Flush()
	done <- struct{}{}
}

func write(input string, prefix string, buckets []FileBucket) {
	fmt.Println("[write] writing output files...")
	f, err := os.Open(input)
	if err != nil {
		panic(err)
	}

	r := csv.NewReader(bufio.NewReader(f))
	writers := make([]*csv.Writer, len(buckets))
	files := make([]*os.File, len(buckets))

	for i := range writers {
		file, err := os.Create(fmt.Sprintf("%s%d.csv", prefix, i + 1))
		if err != nil {
			panic(err)
		}
		files[i] = file
		writers[i] = csv.NewWriter(file)
	}

	// memoize line to bucket for fast O(1) lookup
	lineToBucket := make(map[int]int)
	for i, bucket := range buckets {
		for lineNum := range bucket.LineNums {
			lineToBucket[lineNum] = i
		}
	}

	channels := make([]chan RecordData, len(buckets))
	done := make(chan struct{}, len(buckets))

	defer func(){
		for _, ch := range channels {
			close(ch)
		}

		for i := 0; i < len(buckets); i++ {
			<-done
		}

		for _, w := range writers {
			if err := w.Error(); err != nil {
				fmt.Printf("Error writing to file: %v\n", err)
				os.Exit(1)
			}
		}

		f.Close()

		for _, w := range writers {
			w.Flush()
			if err := w.Error(); err != nil {
				fmt.Printf("Error flushing writer: %v\n", err)
				os.Exit(1)
			}
		}

		for _, file := range files {
			if err := file.Close(); err != nil {
				fmt.Printf("Error closing file: %v\n", err)
				os.Exit(1)
			}
		}
	}()

	for i := range channels {
		channels[i] = make(chan RecordData, 10000) // buffered channel
		go writerRoutine(channels[i], writers[i], done)
	}

	lineNum := 0
	for {
		record, err := r.Read()
		if err != nil {
			break
		}

		if lineNum == 0 {
			for _, w := range writers {
				w.Write(record)
			}
			lineNum++
		}

		bucketIndex, ok := lineToBucket[lineNum]
		if !ok {
			fmt.Printf("Warning: line %d not found in any bucket, skipping...\n", lineNum)
			lineNum++
			continue
		}
		if bucketIndex >= 0 && bucketIndex < len(channels) {
			channels[bucketIndex] <- RecordData{record: record, lineNum: lineNum}
		} else {
			fmt.Printf("Error: bucket index %d out of range for line %d\n", bucketIndex, lineNum)
			os.Exit(1)
		}

		if lineNum % 1000000 == 0 {
			fmt.Printf("[write] %d lines written...\n", lineNum)
		}

		lineNum++
	}



	fmt.Println("[write] all files written successfully\n")

}
