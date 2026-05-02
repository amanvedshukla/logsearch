package gzindex

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"

	pgzip "github.com/klauspost/compress/gzip"
)

// Entry — ek minute ka starting line number
type Entry struct {
	TimeKey    string `json:"t"` // "HH:MM"
	LineNumber int64  `json:"l"` // line number (1-based)
}

type Index struct {
	Entries []Entry `json:"entries"`
}

// IndexPath — gz file ke saath .idx file path
func IndexPath(gzPath string) string {
	return gzPath + ".idx"
}

// NeedsRebuild — check karo index outdated hai ya nahi
func NeedsRebuild(gzPath string) bool {
	idxPath := IndexPath(gzPath)
	gzInfo, err := os.Stat(gzPath)
	if err != nil {
		return true
	}
	idxInfo, err := os.Stat(idxPath)
	if err != nil {
		return true
	}
	return gzInfo.ModTime().After(idxInfo.ModTime())
}

// Build — gz file scan karo, har minute ka first line number save karo
func Build(gzPath string) error {
	f, err := os.Open(gzPath)
	if err != nil {
		return err
	}
	defer f.Close()

	gz, err := pgzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	buf := make([]byte, 4*1024*1024)
	scanner.Buffer(buf, len(buf))

	var entries []Entry
	lastMinute := ""
	var lineNum int64

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Timestamp extract karo — format: "2006-01-02 HH:MM:SS" ya "HH:MM:SS"
		minute := extractMinute(line)
		if minute == "" {
			continue
		}

		if minute != lastMinute {
			entries = append(entries, Entry{
				TimeKey:    minute,
				LineNumber: lineNum,
			})
			lastMinute = minute
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	// Index file mein save karo
	idxPath := IndexPath(gzPath)
	out, err := os.Create(idxPath)
	if err != nil {
		return err
	}
	defer out.Close()

	return json.NewEncoder(out).Encode(Index{Entries: entries})
}

// Load — index file padhو
func Load(gzPath string) (*Index, error) {
	idxPath := IndexPath(gzPath)
	f, err := os.Open(idxPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var idx Index
	if err := json.NewDecoder(f).Decode(&idx); err != nil {
		return nil, err
	}
	return &idx, nil
}

// FindStartLine — given startTime "HH:MM", starting line number return karo
func (idx *Index) FindStartLine(startTime string) int64 {
	if startTime == "" || len(idx.Entries) == 0 {
		return 0
	}
	t := strings.TrimSpace(startTime)
	if len(t) > 5 {
		t = t[:5]
	}

	var result int64 = 0
	for _, e := range idx.Entries {
		if e.TimeKey <= t {
			result = e.LineNumber
		} else {
			break
		}
	}
	// Thoda pehle se start karo — safety buffer
	if result > 1000 {
		result -= 1000
	} else {
		result = 0
	}
	return result
}

// extractMinute — line se HH:MM extract karo
func extractMinute(line string) string {
	if len(line) < 5 {
		return ""
	}
	// Try common formats:
	// "2026-04-01 10:45:23" → index 11:16
	// "10:45:23" → index 0:5
	for _, start := range []int{11, 0} {
		if start+5 <= len(line) {
			t := line[start : start+5]
			if len(t) == 5 && t[2] == ':' &&
				t[0] >= '0' && t[0] <= '2' &&
				t[1] >= '0' && t[1] <= '9' &&
				t[3] >= '0' && t[3] <= '5' &&
				t[4] >= '0' && t[4] <= '9' {
				return t
			}
		}
	}
	return ""
}
