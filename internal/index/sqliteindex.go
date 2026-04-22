package index

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

type SQLiteIndex struct {
	db *sql.DB
}

func NewSQLiteIndex(dbPath string) (*SQLiteIndex, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_keywords (
			file_path TEXT NOT NULL,
			keyword   TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_keyword ON file_keywords(keyword);
	`)
	if err != nil {
		return nil, err
	}
	return &SQLiteIndex{db: db}, nil
}

// IndexFile — streaming version, constant RAM usage regardless of file size
func (s *SQLiteIndex) IndexFile(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()

	// Collect unique words — streaming line by line
	wordSet := make(map[string]bool)
	scanner := bufio.NewScanner(gz)
	buf := make([]byte, 4*1024*1024) // 4MB line buffer
	scanner.Buffer(buf, len(buf))

	for scanner.Scan() {
		line := scanner.Text()
		for _, word := range strings.Fields(line) {
			w := strings.ToLower(strings.Trim(word, ".,!?:;\"'()[]{}"))
			if len(w) >= 3 {
				wordSet[w] = true
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	// Insert in one transaction — fast bulk insert
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	// Delete old entries for this file first
	tx.Exec("DELETE FROM file_keywords WHERE file_path = ?", filePath)

	stmt, err := tx.Prepare("INSERT INTO file_keywords(file_path, keyword) VALUES(?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for word := range wordSet {
		if _, err := stmt.Exec(filePath, word); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLiteIndex) QueryFiles(keyword string) ([]string, error) {
	rows, err := s.db.Query(
		"SELECT DISTINCT file_path FROM file_keywords WHERE keyword = ?",
		strings.ToLower(keyword),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		files = append(files, path)
	}
	return files, nil
}

func (s *SQLiteIndex) QueryFilesForKeywords(keywords []string, mode string) ([]string, error) {
	if len(keywords) == 0 {
		return nil, nil
	}
	if mode == "OR" {
		fileSet := make(map[string]bool)
		for _, kw := range keywords {
			files, err := s.QueryFiles(kw)
			if err != nil {
				return nil, err
			}
			for _, f := range files {
				fileSet[f] = true
			}
		}
		var result []string
		for f := range fileSet {
			result = append(result, f)
		}
		return result, nil
	}
	fileSet := make(map[string]int)
	for _, kw := range keywords {
		files, err := s.QueryFiles(kw)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			fileSet[f]++
		}
	}
	var result []string
	for f, count := range fileSet {
		if count == len(keywords) {
			result = append(result, f)
		}
	}
	return result, nil
}

func (s *SQLiteIndex) Close() error {
	return s.db.Close()
}
