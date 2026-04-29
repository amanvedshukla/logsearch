package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ahocorasick "github.com/BobuSumisu/aho-corasick"
	"logsearch/internal/cache"
	"logsearch/internal/export"
	"logsearch/internal/types"
)

// ─── Config ───────────────────────────────────────────────────────────────────
const (
	DefaultMaxLinesPerFile = 0 // 0 = unlimited
	NoMatchTimeoutSecs     = 500
	MaxMemEntries          = 20
	MaxDiskMB              = 500
)

// ─── Globals ──────────────────────────────────────────────────────────────────
var (
	queryCache    *cache.Cache
	bufPool       = sync.Pool{New: func() interface{} { b := make([]byte, 64<<20); return &b }}
	serverStartTime = time.Now()
)

type memEntry struct {
	matches   []types.Match
	totalSize int64
	files     int
	zipPath   string
	createdAt time.Time
}

var (
	memStore   = map[string]*memEntry{}
	memStoreMu sync.RWMutex
)

func storeInMem(key string, e *memEntry) {
	memStoreMu.Lock()
	defer memStoreMu.Unlock()
	if len(memStore) >= MaxMemEntries {
		var ok, ot = "", time.Time{}
		for k, v := range memStore {
			if ok == "" || v.createdAt.Before(ot) {
				ok, ot = k, v.createdAt
			}
		}
		delete(memStore, ok)
	}
	memStore[key] = e
}

func getFromMem(key string) *memEntry {
	memStoreMu.RLock()
	defer memStoreMu.RUnlock()
	return memStore[key]
}

func queryHash(q types.Query) string {
	q.RequestID = ""
	b, _ := json.Marshal(q)
	return fmt.Sprintf("%x", b)
}

func fmtNum(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1_000_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ─── Auto disk cleanup ────────────────────────────────────────────────────────
func autoClearExports() {
	var totalBytes int64
	type fi struct {
		path    string
		modTime time.Time
	}
	var files []fi
	filepath.Walk("exports", func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalBytes += info.Size()
			files = append(files, fi{p, info.ModTime()})
		}
		return nil
	})
	limit := int64(MaxDiskMB) * 1024 * 1024
	if totalBytes <= limit {
		return
	}
	log.Printf("[CLEANUP] exports/ %.1fMB > %dMB limit", float64(totalBytes)/1e6, MaxDiskMB)
	for i := 0; i < len(files)-1 && totalBytes > limit; i++ {
		oldest := i
		for j := i + 1; j < len(files); j++ {
			if files[j].modTime.Before(files[oldest].modTime) {
				oldest = j
			}
		}
		if info, err := os.Stat(files[oldest].path); err == nil {
			totalBytes -= info.Size()
		}
		os.Remove(files[oldest].path)
		files[oldest] = files[i]
	}
}

// ─── Saved Searches (file-based persistence) ─────────────────────────────────
type SavedSearch struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Keywords  []string `json:"keywords"`
	Mode      string   `json:"mode"`
	Path      string   `json:"path"`
	CreatedAt string   `json:"created_at"`
}

const savedSearchesFile = "saved_searches.json"

func loadSavedSearches() []SavedSearch {
	data, err := os.ReadFile(savedSearchesFile)
	if err != nil {
		return []SavedSearch{}
	}
	var ss []SavedSearch
	json.Unmarshal(data, &ss)
	return ss
}

func saveSavedSearches(ss []SavedSearch) {
	data, _ := json.MarshalIndent(ss, "", "  ")
	os.WriteFile(savedSearchesFile, data, 0644)
}

func savedSearchesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		json.NewEncoder(w).Encode(loadSavedSearches())
	case http.MethodPost:
		var s SavedSearch
		if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		s.ID = fmt.Sprintf("%d", time.Now().UnixNano())
		s.CreatedAt = time.Now().Format("2006-01-02 15:04:05")
		ss := loadSavedSearches()
		ss = append(ss, s)
		saveSavedSearches(ss)
		json.NewEncoder(w).Encode(s)
	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		ss := loadSavedSearches()
		var newSS []SavedSearch
		for _, s := range ss {
			if s.ID != id {
				newSS = append(newSS, s)
			}
		}
		saveSavedSearches(newSS)
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}
}

// ─── /browse ──────────────────────────────────────────────────────────────────
func browseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := filepath.Clean(r.URL.Query().Get("path"))
	if path == "" || path == "." {
		path = "/"
	}
	info, err := os.Stat(path)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "Cannot access: " + err.Error()})
		return
	}
	if !info.IsDir() {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "Not a directory"})
		return
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "Cannot read: " + err.Error()})
		return
	}
	type Entry struct {
		Name    string `json:"name"`
		Path    string `json:"path"`
		IsDir   bool   `json:"is_dir"`
		Size    int64  `json:"size"`
		GzCount int    `json:"gz_count"`
	}
	var items []Entry
	for _, de := range entries {
		fullPath := filepath.Join(path, de.Name())
		entry := Entry{Name: de.Name(), Path: fullPath, IsDir: de.IsDir()}
		if !de.IsDir() {
			if fi, err := de.Info(); err == nil {
				entry.Size = fi.Size()
			}
		} else {
			subs, _ := os.ReadDir(fullPath)
			for _, se := range subs {
				if !se.IsDir() && strings.HasSuffix(se.Name(), ".gz") {
					entry.GzCount++
				}
			}
		}
		items = append(items, entry)
	}
	parent := filepath.Dir(path)
	if parent == path {
		parent = ""
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"path":    path,
		"parent":  parent,
		"entries": items,
	})
}

// ─── Search core ──────────────────────────────────────────────────────────────
type Progress struct {
	FilesTotal    int64
	FilesSearched int64
	LinesScanned  int64
	MatchesFound  int64
}

func searchFiles(
	ctx context.Context,
	gzFiles []string,
	q types.Query,
	trie *ahocorasick.Trie,
	kwLower []string,
	excludeLower []string,
	regexPat *regexp.Regexp,
	progressCh chan<- Progress,
	resultsCh chan<- []types.Match,
	matchWriter *bufio.Writer,
	writeMu *sync.Mutex,
	matchCount *int64,
	maxMatches int64,
) {
	var (
		filesSearched int64
		linesScanned  int64
		matchesFound  int64
		totalFiles    = int64(len(gzFiles))
	)
	var wg sync.WaitGroup

	for _, f := range gzFiles {
		wg.Add(1)
		go func(fpath string) {
			defer wg.Done()
			defer func() {
				atomic.AddInt64(&filesSearched, 1)
				select {
				case progressCh <- Progress{
					FilesTotal: totalFiles, FilesSearched: atomic.LoadInt64(&filesSearched),
					LinesScanned: atomic.LoadInt64(&linesScanned), MatchesFound: atomic.LoadInt64(&matchesFound),
				}:
				default:
				}
			}()

			select {
			case <-ctx.Done():
				resultsCh <- nil
				return
			default:
			}

			fp, err := os.Open(fpath)
			if err != nil {
				resultsCh <- nil
				return
			}
			defer fp.Close()

			gz, err := gzip.NewReader(fp)
			if err != nil {
				resultsCh <- nil
				return
			}
			defer gz.Close()

			bufPtr := bufPool.Get().(*[]byte)
			defer bufPool.Put(bufPtr)

			scanner := bufio.NewScanner(gz)
			scanner.Buffer(*bufPtr, len(*bufPtr))

			contextSize := q.ContextLines
			if contextSize < 0 {
				contextSize = 0
			}
			if contextSize > 10 {
				contextSize = 10
			}

			lineNum := 0
			fileMatchCount := 0
			if contextSize > 0 {
				window := make([]string, 0, contextSize*2+1)
				pendingMatches := []struct {
					line   string
					terms  []string
					winIdx int
				}{}
				lineIdx := 0
				for scanner.Scan() {
					lineIdx++
					line := scanner.Text()
					window = append(window, line)
					atomic.AddInt64(&linesScanned, 1)
					if lineIdx%50000 == 0 {
						select {
						case progressCh <- Progress{
							FilesTotal: totalFiles, FilesSearched: atomic.LoadInt64(&filesSearched),
							LinesScanned: atomic.LoadInt64(&linesScanned), MatchesFound: atomic.LoadInt64(&matchesFound),
						}:
						default:
						}
					}
					select {
					case <-ctx.Done():
						resultsCh <- nil
						return
					default:
					}
					m, terms := matchLine(line, trie, kwLower, excludeLower, regexPat, q)
					if m {
						atomic.AddInt64(&matchesFound, 1)
						fileMatchCount++
						pendingMatches = append(pendingMatches, struct {
							line   string
							terms  []string
							winIdx int
						}{line, terms, lineIdx})
					}
					for len(pendingMatches) > 0 {
						pm := pendingMatches[0]
						if lineIdx >= pm.winIdx+contextSize {
							matchWinPos := lineIdx - pm.winIdx
							ctxStart := len(window) - matchWinPos - contextSize - 1
							if ctxStart < 0 {
								ctxStart = 0
							}
							ctxEnd := len(window)
							var ctxLines []string
							for _, cl := range window[ctxStart:ctxEnd] {
								ctxLines = append(ctxLines, cl)
							}
							ctxStartLine := pm.winIdx - (len(ctxLines) - contextSize - 1)
							if ctxStartLine < 1 {
								ctxStartLine = 1
							}
							_m := types.Match{FilePath: fpath, LineNumber: pm.winIdx, LineText: pm.line, MatchedTerms: pm.terms, ContextLines: ctxLines, ContextStart: ctxStartLine}
							if matchWriter != nil {
								writeMu.Lock()
								if _d, _e := json.Marshal(_m); _e == nil {
									matchWriter.Write(_d)
									matchWriter.WriteByte('\n')
								}
								writeMu.Unlock()
							}
							newCnt := atomic.AddInt64(matchCount, 1)
							pendingMatches = pendingMatches[1:]
							if maxMatches > 0 && newCnt >= maxMatches {
								resultsCh <- nil
								return
							}
						} else {
							break
						}
					}
					if len(window) > contextSize*2+2 {
						window = window[1:]
					}
				}
				for _, pm := range pendingMatches {
					ctxStart := 0
					ctxEnd := len(window)
					var ctxLines []string
					for _, cl := range window[ctxStart:ctxEnd] {
						ctxLines = append(ctxLines, cl)
					}
					_m := types.Match{FilePath: fpath, LineNumber: pm.winIdx, LineText: pm.line, MatchedTerms: pm.terms, ContextLines: ctxLines, ContextStart: pm.winIdx}
					if matchWriter != nil {
						writeMu.Lock()
						if _d, _e := json.Marshal(_m); _e == nil {
							matchWriter.Write(_d)
							matchWriter.WriteByte('\n')
						}
						writeMu.Unlock()
					}
					atomic.AddInt64(matchCount, 1)
				}
			} else {
				for scanner.Scan() {
					lineNum++
					select {
					case <-ctx.Done():
						resultsCh <- nil
						return
					default:
					}
					if q.MaxLinesPerFile > 0 && lineNum > q.MaxLinesPerFile {
						break
					}
					if q.MaxLinesPerFile > 0 && fileMatchCount >= q.MaxLinesPerFile {
						break
					}
					atomic.AddInt64(&linesScanned, 1)
					if lineNum%50000 == 0 {
						select {
						case progressCh <- Progress{
							FilesTotal: totalFiles, FilesSearched: atomic.LoadInt64(&filesSearched),
							LinesScanned: atomic.LoadInt64(&linesScanned), MatchesFound: atomic.LoadInt64(&matchesFound),
						}:
						default:
						}
					}
					line := scanner.Text()
					m, terms := matchLine(line, trie, kwLower, excludeLower, regexPat, q)
					if m {
						atomic.AddInt64(&matchesFound, 1)
						fileMatchCount++
						_m := types.Match{FilePath: fpath, LineNumber: lineNum, LineText: line, MatchedTerms: terms}
						if matchWriter != nil {
							writeMu.Lock()
							if _d, _e := json.Marshal(_m); _e == nil {
								matchWriter.Write(_d)
								matchWriter.WriteByte('\n')
							}
							writeMu.Unlock()
						}
						newCnt := atomic.AddInt64(matchCount, 1)
						if maxMatches > 0 && newCnt >= maxMatches {
							resultsCh <- nil
							return
						}
					}
				}
			}
			resultsCh <- nil
		}(f)
	}

	wg.Wait()
	close(progressCh)
	close(resultsCh)
}

func matchLine(
	line string,
	trie *ahocorasick.Trie,
	kwLower []string,
	excludeLower []string,
	regexPat *regexp.Regexp,
	q types.Query,
) (bool, []string) {
	if (q.StartTime != "" || q.EndTime != "") && len(line) >= 19 {
		lineTime := line[11:16]
		start := strings.TrimSpace(q.StartTime)
		end := strings.TrimSpace(q.EndTime)
		if len(start) >= 5 {
			start = start[:5]
		}
		if len(end) >= 5 {
			end = end[:5]
		}
		if start != "" && lineTime < start {
			return false, nil
		}
		if end != "" && lineTime > end {
			return false, nil
		}
	}
	lower := strings.ToLower(line)

	for _, ex := range excludeLower {
		if strings.Contains(lower, ex) {
			return false, nil
		}
	}

	if regexPat != nil {
		if regexPat.MatchString(line) {
			return true, []string{"regex"}
		}
		return false, nil
	}

	hits := trie.MatchString(lower)
	if len(hits) == 0 {
		return false, nil
	}
	foundMap := map[string]bool{}
	for _, h := range hits {
		foundMap[string(h.Match())] = true
	}
	var terms []string
	for i, kw := range kwLower {
		if foundMap[kw] {
			terms = append(terms, q.Keywords[i])
		}
	}
	if q.Mode == "AND" && len(terms) != len(kwLower) {
		return false, nil
	}
	if q.Mode == "OR" && len(terms) == 0 {
		return false, nil
	}
	return true, terms
}

// ─── /search/stream ───────────────────────────────────────────────────────────
func searchSSEHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", 500)
		return
	}

	send := func(event string, data interface{}) {
		b, _ := json.Marshal(data)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
		flusher.Flush()
	}

	queryJSON := r.URL.Query().Get("q")
	if queryJSON == "" {
		send("error", map[string]string{"message": "missing query"})
		return
	}
	var q types.Query
	if err := json.Unmarshal([]byte(queryJSON), &q); err != nil {
		send("error", map[string]string{"message": "invalid query JSON"})
		return
	}
	if len(q.Keywords) == 0 && q.RegexPattern == "" {
		send("error", map[string]string{"message": "keywords or regex required"})
		return
	}
	if q.Mode != "AND" && q.Mode != "OR" {
		q.Mode = "OR"
	}
	if q.SearchPath == "" {
		send("error", map[string]string{"message": "search_path required"})
		return
	}

	log.Printf("[SEARCH] keywords=%v regex=%s exclude=%v mode=%s path=%s",
		q.Keywords, q.RegexPattern, q.ExcludeKeywords, q.Mode, q.SearchPath)

	cacheKey := queryHash(q)
	if cached := queryCache.GetFilePath(cacheKey); cached != nil {
		log.Printf("[SEARCH] cache hit")
		send("cache_hit", map[string]interface{}{
			"total_matches":  cached.TotalMatches,
			"total_files":    cached.TotalFiles,
			"files_searched": cached.TotalFiles,
			"searched_size":  cached.SearchedSize,
			"download_url":   "",
			"cache_hit":      true,
		})
		return
	}

	send("start", map[string]string{"message": "Scanning for .gz files..."})

	var gzFiles []string
	var totalSize int64
	fi, err := os.Stat(q.SearchPath)
	if err != nil {
		send("error", map[string]string{"message": "Cannot access: " + err.Error()})
		return
	}
	if !fi.IsDir() {
		if strings.HasSuffix(q.SearchPath, ".gz") {
			gzFiles = []string{q.SearchPath}
			totalSize = fi.Size()
		} else {
			send("error", map[string]string{"message": "File must be .gz"})
			return
		}
	} else {
		filepath.Walk(q.SearchPath, func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && strings.HasSuffix(p, ".gz") {
				gzFiles = append(gzFiles, p)
				totalSize += info.Size()
			}
			return nil
		})
	}

	if q.SearchOrder == "latest" {
		sort.Slice(gzFiles, func(i, j int) bool { return gzFiles[i] > gzFiles[j] })
	} else {
		sort.Slice(gzFiles, func(i, j int) bool { return gzFiles[i] < gzFiles[j] })
	}

	if len(gzFiles) == 0 {
		send("done", map[string]interface{}{
			"total_matches": 0, "total_files": 0,
			"files_searched": 0, "searched_size": 0,
			"no_match_msg": "No .gz files found in the selected path",
		})
		return
	}

	send("progress", map[string]interface{}{
		"message":       fmt.Sprintf("Found %d .gz files (%.2f GB) — searching...", len(gzFiles), float64(totalSize)/1e9),
		"files_total":   len(gzFiles),
		"searched_size": totalSize,
	})

	kwLower := make([]string, len(q.Keywords))
	for i, k := range q.Keywords {
		kwLower[i] = strings.ToLower(k)
	}
	excludeLower := make([]string, len(q.ExcludeKeywords))
	for i, k := range q.ExcludeKeywords {
		excludeLower[i] = strings.ToLower(k)
	}

	var regexPat *regexp.Regexp
	if q.RegexPattern != "" {
		var err error
		if q.CaseSensitive {
			regexPat, err = regexp.Compile(q.RegexPattern)
		} else {
			regexPat, err = regexp.Compile("(?i)" + q.RegexPattern)
		}
		if err != nil {
			send("error", map[string]string{"message": "Invalid regex: " + err.Error()})
			return
		}
	}

	builder := ahocorasick.NewTrieBuilder()
	if regexPat == nil {
		for _, k := range kwLower {
			builder.AddPattern([]byte(k))
		}
	} else {
		builder.AddPattern([]byte("x"))
	}
	trie := builder.Build()

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	progressCh := make(chan Progress, 2)
	resultsCh := make(chan []types.Match, len(gzFiles))

	os.MkdirAll("results_cache", 0755)
	tmpFile, _ := os.CreateTemp("results_cache", "results_*.ndjson")
	var matchWriter *bufio.Writer
	var writeMu sync.Mutex
	var matchCount int64
	maxMatches := q.MaxMatches
	if tmpFile != nil {
		matchWriter = bufio.NewWriterSize(tmpFile, 4*1024*1024)
	}
	go searchFiles(ctx, gzFiles, q, trie, kwLower, excludeLower, regexPat, progressCh, resultsCh, matchWriter, &writeMu, &matchCount, maxMatches)

	searchStart := time.Now()
	noMatchTimer := time.NewTimer(time.Duration(NoMatchTimeoutSecs) * time.Second)
	defer noMatchTimer.Stop()
	timedOut := false
	var lastProg Progress

	for prog := range progressCh {
		if timedOut {
			continue
		}
		lastProg = prog
		elapsed := time.Since(searchStart).Milliseconds()

		if atomic.LoadInt64(&prog.MatchesFound) == 0 {
			select {
			case <-noMatchTimer.C:
				timedOut = true
				cancelFn()
				noMsg := fmt.Sprintf("🔍 Keyword '%s' not found after %d seconds — search stopped. Try different keywords.",
					strings.Join(q.Keywords, ", "), NoMatchTimeoutSecs)
				if q.RegexPattern != "" {
					noMsg = fmt.Sprintf("🔍 Regex '%s' not found after %d seconds — search stopped.", q.RegexPattern, NoMatchTimeoutSecs)
				}
				send("done", map[string]interface{}{
					"total_matches":  0,
					"total_files":    prog.FilesTotal,
					"files_searched": prog.FilesSearched,
					"searched_size":  totalSize,
					"elapsed_ms":     elapsed,
					"download_url":   "",
					"cache_hit":      false,
					"no_match_msg":   noMsg,
				})
				return
			default:
			}
		} else {
			noMatchTimer.Stop()
		}

		pct := prog.FilesSearched * 100 / max64(prog.FilesTotal, 1)
		send("progress", map[string]interface{}{
			"files_total":    prog.FilesTotal,
			"files_searched": prog.FilesSearched,
			"lines_scanned":  prog.LinesScanned,
			"matches_found":  prog.MatchesFound,
			"elapsed_ms":     elapsed,
			"percent":        pct,
			"message": fmt.Sprintf("Searching... %d/%d files | %s lines | %d matches | %dms",
				prog.FilesSearched, prog.FilesTotal,
				fmtNum(prog.LinesScanned), prog.MatchesFound, elapsed),
		})
	}

	_ = lastProg

	for range resultsCh {
	}
	elapsed := time.Since(searchStart).Milliseconds()
	if matchWriter != nil {
		writeMu.Lock()
		matchWriter.Flush()
		writeMu.Unlock()
	}
	filePath := ""
	if tmpFile != nil {
		tmpFile.Close()
		filePath = tmpFile.Name()
	}
	totalMatchCount := int(atomic.LoadInt64(&matchCount))
	noMatchMsg := ""
	if totalMatchCount == 0 {
		noMatchMsg = fmt.Sprintf("No matches found for '%s' in %d files (%dms)", strings.Join(q.Keywords, ", "), len(gzFiles), elapsed)
		if filePath != "" {
			os.Remove(filePath)
			filePath = ""
		}
	}
	log.Printf("[SEARCH] done — %d matches in %dms", totalMatchCount, elapsed)
	if filePath != "" {
		queryCache.SetFilePath(cacheKey, cache.CacheEntry{FilePath: filePath, TotalMatches: totalMatchCount, TotalFiles: len(gzFiles), SearchedSize: totalSize})
		go cache.CleanOldResultFiles("results_cache", 5120)
	}
	send("done", map[string]interface{}{
		"total_matches":  totalMatchCount,
		"total_files":    len(gzFiles),
		"files_searched": len(gzFiles),
		"searched_size":  totalSize,
		"elapsed_ms":     elapsed,
		"download_url":   "",
		"cache_hit":      false,
		"no_match_msg":   noMatchMsg,
	})
}

// ─── /search/page ─────────────────────────────────────────────────────────────
func pageHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var q types.Query
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "invalid JSON"})
		return
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 25
	}
	if pageSize > 500 {
		pageSize = 500
	}
	entry := queryCache.GetFilePath(queryHash(q))
	if entry == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "no results — run search first"})
		return
	}
	matches, err := cache.ReadMatchesPage(entry.FilePath, page, pageSize)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "read error: " + err.Error()})
		return
	}
	totalPg := (entry.TotalMatches + pageSize - 1) / pageSize
	if totalPg < 1 {
		totalPg = 1
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"matches":        matches,
		"total_matches":  entry.TotalMatches,
		"total_files":    entry.TotalFiles,
		"files_searched": entry.TotalFiles,
		"searched_size":  entry.SearchedSize,
		"cache_hit":      false,
		"download_url":   "",
		"page":           page,
		"page_size":      pageSize,
		"total_pages":    totalPg,
	})
}

// ─── Download handlers ────────────────────────────────────────────────────────
func csvHandler(w http.ResponseWriter, r *http.Request) {
	var q types.Query
	json.NewDecoder(r.Body).Decode(&q)
	entry := queryCache.GetFilePath(queryHash(q))
	if entry == nil {
		http.Error(w, "no results", 404)
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=logsearch_results.csv")
	cw := csv.NewWriter(w)
	cw.Write([]string{"file", "line_number", "matched_terms", "log_message"})
	allDlMatches, _ := cache.ReadMatchesPage(entry.FilePath, 1, entry.TotalMatches)
	for _, m := range allDlMatches {
		fname := filepath.Base(m.FilePath)
		cw.Write([]string{fname, strconv.Itoa(m.LineNumber), strings.Join(m.MatchedTerms, "|"), m.LineText})
	}
	cw.Flush()
}

func txtHandler(w http.ResponseWriter, r *http.Request) {
	var q types.Query
	json.NewDecoder(r.Body).Decode(&q)
	entry := queryCache.GetFilePath(queryHash(q))
	if entry == nil {
		http.Error(w, "no results", 404)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=logsearch_results.txt")
	allDlMatches, _ := cache.ReadMatchesPage(entry.FilePath, 1, entry.TotalMatches)
	for _, m := range allDlMatches {
		fmt.Fprintf(w, "%s\n", m.LineText)
	}
}

func jsonDLHandler(w http.ResponseWriter, r *http.Request) {
	var q types.Query
	json.NewDecoder(r.Body).Decode(&q)
	entry := queryCache.GetFilePath(queryHash(q))
	if entry == nil {
		http.Error(w, "no results", 404)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", "attachment; filename=logsearch_results.json")
	type em struct {
		File         string   `json:"file"`
		LineNumber   int      `json:"line_number"`
		MatchedTerms []string `json:"matched_terms"`
		LogMessage   string   `json:"log_message"`
	}
	var out []em
	allDlMatches, _ := cache.ReadMatchesPage(entry.FilePath, 1, entry.TotalMatches)
	for _, m := range allDlMatches {
		out = append(out, em{
			File:         filepath.Base(m.FilePath),
			LineNumber:   m.LineNumber,
			MatchedTerms: m.MatchedTerms,
			LogMessage:   m.LineText,
		})
	}
	json.NewEncoder(w).Encode(out)
}

func pageDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PageData []string `json:"__page_data__"`
		Format   string   `json:"format"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	switch body.Format {
	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=logsearch_page.csv")
		cw := csv.NewWriter(w)
		cw.Write([]string{"log_message"})
		for _, line := range body.PageData {
			cw.Write([]string{line})
		}
		cw.Flush()
	case "json":
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=logsearch_page.json")
		type row struct {
			LogMessage string `json:"log_message"`
		}
		var out []row
		for _, line := range body.PageData {
			out = append(out, row{LogMessage: line})
		}
		json.NewEncoder(w).Encode(out)
	default:
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Content-Disposition", "attachment; filename=logsearch_page.txt")
		for _, line := range body.PageData {
			fmt.Fprintf(w, "%s\n", line)
		}
	}
}

func zipHandler(w http.ResponseWriter, r *http.Request) {
	var q types.Query
	json.NewDecoder(r.Body).Decode(&q)
	entry := queryCache.GetFilePath(queryHash(q))
	if entry == nil {
		http.Error(w, "no results", 404)
		return
	}
	allMatches, err := cache.ReadMatchesPage(entry.FilePath, 1, entry.TotalMatches)
	if err != nil {
		http.Error(w, "read error: "+err.Error(), 500)
		return
	}
	result := &types.SearchResult{
		Matches:       allMatches,
		TotalFiles:    entry.TotalFiles,
		FilesSearched: entry.TotalFiles,
	}
	zipPath, err := export.CreateZip(result)
	if err != nil {
		http.Error(w, "zip error: "+err.Error(), 500)
		return
	}
	defer os.Remove(zipPath)
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=logsearch_results.zip")
	http.ServeFile(w, r, zipPath)
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path[1:]
	if _, err := os.Stat(p); err != nil {
		http.Error(w, "Not found", 404)
		return
	}
	w.Header().Set("Content-Disposition", "attachment; filename=logsearch_results.zip")
	http.ServeFile(w, r, p)
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFiles("ui/templates/index.html")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	tmpl.Execute(w, nil)
}

// ─── /stats/file ──────────────────────────────────────────────────────────────
func fileStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var q types.Query
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "invalid JSON"})
		return
	}

	entry := queryCache.GetFilePath(queryHash(q))
	if entry == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "no results — run search first"})
		return
	}

	allMatches, err := cache.ReadMatchesPage(entry.FilePath, 1, entry.TotalMatches)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "read error: " + err.Error()})
		return
	}

	type CountMap map[string]int

	errorCodes := CountMap{}
	apiEndpoints := CountMap{}
	banks := CountMap{}
	users := CountMap{}
	hourBuckets := CountMap{}
	matchedTerms := CountMap{}

	for _, m := range allMatches {
		line := m.LineText
		parts := strings.Split(line, "|")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if strings.HasPrefix(p, "ERROR_CODE:") {
				errorCodes[strings.TrimPrefix(p, "ERROR_CODE:")]++
			} else if strings.HasPrefix(p, "API:") {
				apiEndpoints[strings.TrimPrefix(p, "API:")]++
			} else if strings.HasPrefix(p, "BANK:") {
				banks[strings.TrimPrefix(p, "BANK:")]++
			} else if strings.HasPrefix(p, "USER:") {
				users[strings.TrimPrefix(p, "USER:")]++
			}
		}
		if len(line) >= 13 {
			hourBuckets[line[11:13]+":00"]++
		}
		for _, t := range m.MatchedTerms {
			matchedTerms[t]++
		}
	}

	toSlice := func(cm CountMap, limit int) []map[string]interface{} {
		type kv struct {
			k string
			v int
		}
		var kvs []kv
		for k, v := range cm {
			kvs = append(kvs, kv{k, v})
		}
		sort.Slice(kvs, func(i, j int) bool { return kvs[i].v > kvs[j].v })
		var out []map[string]interface{}
		for i, x := range kvs {
			if limit > 0 && i >= limit {
				break
			}
			out = append(out, map[string]interface{}{"label": x.k, "count": x.v})
		}
		return out
	}

	var timeline []map[string]interface{}
	for h := 0; h < 24; h++ {
		key := fmt.Sprintf("%02d:00", h)
		cnt := hourBuckets[key]
		if cnt > 0 {
			timeline = append(timeline, map[string]interface{}{"label": key, "count": cnt})
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_matches": entry.TotalMatches,
		"total_files":   entry.TotalFiles,
		"error_codes":   toSlice(errorCodes, 10),
		"api_endpoints": toSlice(apiEndpoints, 10),
		"banks":         toSlice(banks, 10),
		"top_users":     toSlice(users, 10),
		"timeline":      timeline,
		"matched_terms": toSlice(matchedTerms, 20),
		"search_path":   q.SearchPath,
		"keywords":      q.Keywords,
	})
}

// ─── /metrics ─────────────────────────────────────────────────────────────────
type cpuSnapshot struct {
	user, nice, system, idle, iowait, irq, softirq uint64
}

func readProcStat() (cpuSnapshot, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuSnapshot{}, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "cpu ") {
			var s cpuSnapshot
			fmt.Sscanf(line, "cpu  %d %d %d %d %d %d %d",
				&s.user, &s.nice, &s.system, &s.idle,
				&s.iowait, &s.irq, &s.softirq)
			return s, nil
		}
	}
	return cpuSnapshot{}, fmt.Errorf("cpu line not found")
}

func cpuPercent() float64 {
	s1, err := readProcStat()
	if err != nil {
		return -1
	}
	time.Sleep(200 * time.Millisecond)
	s2, err := readProcStat()
	if err != nil {
		return -1
	}
	total1 := s1.user + s1.nice + s1.system + s1.idle + s1.iowait + s1.irq + s1.softirq
	total2 := s2.user + s2.nice + s2.system + s2.idle + s2.iowait + s2.irq + s2.softirq
	idle1 := s1.idle + s1.iowait
	idle2 := s2.idle + s2.iowait
	dTotal := float64(total2 - total1)
	dIdle := float64(idle2 - idle1)
	if dTotal == 0 {
		return 0
	}
	return (1.0 - dIdle/dTotal) * 100.0
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	cpu := cpuPercent()
	uptime := time.Since(serverStartTime)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"uptime_seconds": int64(uptime.Seconds()),
		"uptime_human":   fmtUptime(uptime),
		"goroutines":     runtime.NumGoroutine(),
		"cpu_percent":    fmt.Sprintf("%.1f", cpu),
		"heap_alloc_mb":  fmt.Sprintf("%.2f", float64(ms.HeapAlloc)/1e6),
		"heap_sys_mb":    fmt.Sprintf("%.2f", float64(ms.HeapSys)/1e6),
		"heap_inuse_mb":  fmt.Sprintf("%.2f", float64(ms.HeapInuse)/1e6),
		"stack_inuse_mb": fmt.Sprintf("%.2f", float64(ms.StackInuse)/1e6),
		"total_alloc_mb": fmt.Sprintf("%.2f", float64(ms.TotalAlloc)/1e6),
		"sys_mb":         fmt.Sprintf("%.2f", float64(ms.Sys)/1e6),
		"num_gc":         ms.NumGC,
		"gc_pause_ms":    fmt.Sprintf("%.2f", float64(ms.PauseTotalNs)/1e6),
		"next_gc_mb":     fmt.Sprintf("%.2f", float64(ms.NextGC)/1e6),
		"cache_entries":  getCacheEntryCount(),
	})
}

func fmtUptime(d time.Duration) string {
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func getCacheEntryCount() int {
	entries, err := os.ReadDir("results_cache")
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() {
			count++
		}
	}
	return count
}


// ─── /cache/clear ─────────────────────────────────────────────────────────────
func clearCacheHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "POST required"})
		return
	}
	deletedFiles := 0
	entries, err := os.ReadDir("results_cache")
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				if os.Remove(filepath.Join("results_cache", e.Name())) == nil {
					deletedFiles++
				}
			}
		}
	}
	queryCache.ClearAll()
	log.Printf("[CACHE] Cleared %d result files", deletedFiles)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":            true,
		"deleted_files": deletedFiles,
		"message":       fmt.Sprintf("Cache cleared — %d files deleted", deletedFiles),
	})
}
// ─── main ─────────────────────────────────────────────────────────────────────
func main() {
	var err error
	queryCache, err = cache.NewCache("cache.db")
	if err != nil {
		log.Fatalf("Cache: %v", err)
	}
	defer queryCache.Close()

	os.MkdirAll("exports", 0755)

	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/browse", browseHandler)
	http.HandleFunc("/search/stream", searchSSEHandler)
	http.HandleFunc("/search/page", pageHandler)
	http.HandleFunc("/download/zip", zipHandler)
	http.HandleFunc("/download/csv", csvHandler)
	http.HandleFunc("/download/txt", txtHandler)
	http.HandleFunc("/download/json", jsonDLHandler)
	http.HandleFunc("/download/page", pageDownloadHandler)
	http.HandleFunc("/exports/", downloadHandler)
	http.HandleFunc("/saved-searches", savedSearchesHandler)
	// ── NEW ──
	http.HandleFunc("/stats/file", fileStatsHandler)
	http.HandleFunc("/metrics", metricsHandler)
	http.HandleFunc("/cache/clear", clearCacheHandler)

	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║   LogSearch Engine v3.0              ║")
	fmt.Println("║   http://localhost:8080              ║")
	fmt.Println("╚══════════════════════════════════════╝")

	srv := &http.Server{
		Addr:         ":8080",
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 600 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}
