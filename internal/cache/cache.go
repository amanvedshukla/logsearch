package cache

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"logsearch/internal/types"
)

type Cache struct{ db *badger.DB }

type CacheEntry struct {
	FilePath     string `json:"file_path"`
	TotalMatches int    `json:"total_matches"`
	TotalFiles   int    `json:"total_files"`
	SearchedSize int64  `json:"searched_size"`
}

func NewCache(dbPath string) (*Cache, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil { return nil, err }
	return &Cache{db: db}, nil
}

func (c *Cache) Close() error { return c.db.Close() }

func QueryHash(query types.Query) string {
	raw := fmt.Sprintf("%v|%v|%s|%s|%s|%s|%s|%s|%v|%d",
		query.Keywords, query.ExcludeKeywords,
		query.RegexPattern, query.StartTime, query.EndTime,
		query.Mode, query.SearchPath, query.SearchOrder,
		query.CaseSensitive, query.MaxLinesPerFile)
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", h)
}

func (c *Cache) SetFilePath(key string, entry CacheEntry) {
	data, _ := json.Marshal(entry)
	_ = c.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("fp:"+key), data)
	})
}

func (c *Cache) GetFilePath(key string) *CacheEntry {
	var entry CacheEntry
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("fp:" + key))
		if err != nil { return err }
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &entry)
		})
	})
	if err != nil { return nil }
	if _, err := os.Stat(entry.FilePath); err != nil { return nil }
	return &entry
}

func WriteMatchesToFile(matches []types.Match, dir string) (string, error) {
	os.MkdirAll(dir, 0755)
	f, err := os.CreateTemp(dir, "results_*.ndjson")
	if err != nil { return "", err }
	defer f.Close()
	w := bufio.NewWriterSize(f, 4*1024*1024)
	enc := json.NewEncoder(w)
	for _, m := range matches {
		if err := enc.Encode(m); err != nil { return "", err }
	}
	return f.Name(), w.Flush()
}

func ReadMatchesPage(filePath string, page, pageSize int) ([]types.Match, error) {
	f, err := os.Open(filePath)
	if err != nil { return nil, err }
	defer f.Close()
	start := (page - 1) * pageSize
	end := start + pageSize
	var matches []types.Match
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 4*1024*1024)
	scanner.Buffer(buf, len(buf))
	lineNum := 0
	for scanner.Scan() {
		if lineNum >= end { break }
		if lineNum >= start {
			var m types.Match
			if err := json.Unmarshal(scanner.Bytes(), &m); err == nil {
				matches = append(matches, m)
			}
		}
		lineNum++
	}
	return matches, scanner.Err()
}

func CleanOldResultFiles(dir string, maxMB int64) {
	var total int64
	type fi struct{ path string; size int64; modTime time.Time }
	var files []fi
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
			files = append(files, fi{p, info.Size(), info.ModTime()})
		}
		return nil
	})
	limit := maxMB * 1024 * 1024
	if total <= limit { return }
	for i := 0; i < len(files)-1; i++ {
		for j := i + 1; j < len(files); j++ {
			if files[j].modTime.Before(files[i].modTime) {
				files[i], files[j] = files[j], files[i]
			}
		}
	}
	log.Printf("[CACHE] %.1fMB > %dMB — cleaning", float64(total)/1e6, maxMB)
	cutoff := time.Now().Add(-2 * time.Minute)
	for i := 0; i < len(files) && total > limit; i++ {
		if files[i].modTime.After(cutoff) { continue }
		total -= files[i].size
		os.Remove(files[i].path)
	}
}

func (c *Cache) Get(key string) *types.SearchResult       { return nil }
func (c *Cache) Set(key string, result *types.SearchResult) {}
func (c *Cache) ForEach(fn func(string, *types.SearchResult)) {}

func (c *Cache) ClearAll() {
	_ = c.db.DropAll()
}
