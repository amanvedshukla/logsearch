package filter

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// GetFilesInTimeRange — directly construct folder paths from time range
// No directory scanning — O(1) path construction
func GetFilesInTimeRange(logsRoot string, startTime string, endTime string) ([]string, error) {
	// Parse times — format: "2006-01-02 15:04"
	start, err := time.Parse("2006-01-02 15:04", startTime)
	if err != nil {
		// Try date-only format
		start, err = time.Parse("2006-01-02", startTime)
		if err != nil {
			return nil, fmt.Errorf("invalid start_time format: %s", startTime)
		}
	}

	end, err := time.Parse("2006-01-02 15:04", endTime)
	if err != nil {
		end, err = time.Parse("2006-01-02", endTime)
		if err != nil {
			return nil, fmt.Errorf("invalid end_time format: %s", endTime)
		}
		// If date only, end of that day
		end = end.Add(23*time.Hour + 59*time.Minute)
	}

	var files []string

	// Walk hour by hour — construct path directly
	current := start
	for !current.After(end) {
		// Path: logsRoot/YYYY/MM/DD/HH/
		folderPath := filepath.Join(
			logsRoot,
			fmt.Sprintf("%04d", current.Year()),
			fmt.Sprintf("%02d", current.Month()),
			fmt.Sprintf("%02d", current.Day()),
			fmt.Sprintf("%02d", current.Hour()),
		)

		// Check if folder exists
		if _, err := os.Stat(folderPath); err == nil {
			// Get all .gz files in this folder
			matches, _ := filepath.Glob(filepath.Join(folderPath, "*.gz"))
			files = append(files, matches...)
		}

		current = current.Add(time.Hour)
	}

	return files, nil
}
