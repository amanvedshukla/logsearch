//go:build ignore

package main

import (
	"fmt"
	"path/filepath"

	"logsearch/internal/index"
)

func main() {
	idx, err := index.NewSQLiteIndex("logsearch.db")
	if err != nil {
		panic(err)
	}
	defer idx.Close()

	files, _ := filepath.Glob("testlogs/2026/04/07/*/*.gz")
	fmt.Printf("Indexing %d files...\n", len(files))

	for _, f := range files {
		if err := idx.IndexFile(f); err != nil {
			fmt.Printf("Error indexing %s: %v\n", f, err)
		} else {
			fmt.Printf("Indexed: %s\n", f)
		}
	}

	fmt.Println("Done! logsearch.db created.")
}
