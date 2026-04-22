package export

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"logsearch/internal/types"
)

// CreateZip — compress all matches into a ZIP file, return file path
func CreateZip(result *types.SearchResult) (string, error) {
	if err := os.MkdirAll("exports", 0755); err != nil {
		return "", err
	}

	timestamp := time.Now().Format("20060102-150405")
	zipName := fmt.Sprintf("exports/results-%s-%s.zip", timestamp, result.RequestID)

	zipFile, err := os.Create(zipName)
	if err != nil {
		return "", err
	}
	defer zipFile.Close()

	zw := zip.NewWriter(zipFile)
	defer zw.Close()

	// File 1: matches.txt — human readable
	txtWriter, err := zw.Create("matches.txt")
	if err != nil {
		return "", err
	}

	fmt.Fprintf(txtWriter, "Log Search Results\n")
	fmt.Fprintf(txtWriter, "==================\n")
	fmt.Fprintf(txtWriter, "Request ID   : %s\n", result.RequestID)
	fmt.Fprintf(txtWriter, "Total Matches: %d\n", len(result.Matches))
	fmt.Fprintf(txtWriter, "Cache Hit    : %v\n", result.CacheHit)
	fmt.Fprintf(txtWriter, "==================\n\n")

	for i, m := range result.Matches {
		fmt.Fprintf(txtWriter, "[%d] File: %s\n", i+1, filepath.Base(m.FilePath))
		fmt.Fprintf(txtWriter, "    Line %d: %s\n", m.LineNumber, m.LineText)
		fmt.Fprintf(txtWriter, "    Terms: %v\n\n", m.MatchedTerms)
	}

	// File 2: results.json — machine readable
	jsonWriter, err := zw.Create("results.json")
	if err != nil {
		return "", err
	}
	encoder := json.NewEncoder(jsonWriter)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return "", err
	}

	return zipName, nil
}
