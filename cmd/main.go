package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"logsearch/internal/cache"
	"logsearch/internal/export"
	"logsearch/internal/filter"
	"logsearch/internal/index"
	"logsearch/internal/search"
	"logsearch/internal/types"
)

var (
	sqlIndex   *index.SQLiteIndex
	queryCache *cache.Cache
)

func searchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	// ── LAYER 1: Input Validation ─────────────────────────────
	var query types.Query
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if len(query.Keywords) == 0 {
		http.Error(w, "keywords cannot be empty", http.StatusBadRequest)
		return
	}
	if query.Mode != "AND" && query.Mode != "OR" {
		query.Mode = "OR"
	}
	if query.StartTime == "" {
		query.StartTime = "2026-04-07"
	}
	if query.EndTime == "" {
		query.EndTime = "2026-04-07"
	}

	log.Printf("[INFO] Search started — keywords: %v, mode: %s, request_id: %s",
		query.Keywords, query.Mode, query.RequestID)

	// ── LAYER 4: Cache Check (before any work) ────────────────
	cacheKey := cache.QueryHash(query)
	if cached := queryCache.Get(cacheKey); cached != nil {
		cached.CacheHit = true
		log.Printf("[CACHE] HIT — returning cached result, request_id: %s", query.RequestID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cached)
		return
	}
	log.Printf("[CACHE] MISS — running full pipeline, request_id: %s", query.RequestID)

	// ── LAYER 2: Filter Pipeline ──────────────────────────────
	candidates, err := filter.FilterPipeline(
		"testlogs",
		query.StartTime,
		query.EndTime,
		query.Keywords,
		query.Mode,
		sqlIndex,
	)
	if err != nil {
		log.Printf("[ERROR] Filter pipeline: %v", err)
		http.Error(w, "Filter error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("[INFO] Filter complete — %d candidate files", len(candidates))

	// ── LAYER 3: Parallel Search ──────────────────────────────
	var matches []types.Match
	if len(candidates) > 0 {
		trie := search.BuildTrie(query.Keywords)
		matches = search.ParallelSearch(candidates, trie, query.Keywords, query.Mode, 0, 0)
	}
	log.Printf("[INFO] Search complete — %d matches found", len(matches))

	result := &types.SearchResult{
		RequestID:     query.RequestID,
		Matches:       matches,
		TotalFiles:    len(candidates),
		FilesSearched: len(candidates),
		CacheHit:      false,
	}

	// ── LAYER 4: Store in Cache ───────────────────────────────
	queryCache.Set(cacheKey, result)
	log.Printf("[CACHE] Result stored for future requests")

	// ── LAYER 5: ZIP Export (async) ────────────────────────────
	go func(r *types.SearchResult) {
		zipPath, err := export.CreateZip(r)
		if err != nil {
			log.Printf("[EXPORT] ZIP error: %v", err)
		} else {
			log.Printf("[EXPORT] ZIP created: %s", zipPath)
		}
	}(result)

	// Response — summary only, matches served via /search/page
	response := map[string]interface{}{
		"request_id":     result.RequestID,
		"total_matches":  len(result.Matches),
		"total_files":    result.TotalFiles,
		"files_searched": result.FilesSearched,
		"cache_hit":      result.CacheHit,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// downloadHandler — serve ZIP files
func downloadHandler(w http.ResponseWriter, r *http.Request) {
	filePath := r.URL.Path[1:] // strip leading /
	if _, err := os.Stat(filePath); err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Disposition", "attachment; filename=results.zip")
	http.ServeFile(w, r, filePath)
}

func main() {
	if _, err := os.Stat("testlogs"); err != nil {
		log.Fatal("Run from project root: cd ~/logsearch && go run cmd/main.go")
	}

	// Open SQLite index
	var err error
	sqlIndex, err = index.NewSQLiteIndex("logsearch.db")
	if err != nil {
		log.Fatalf("SQLite open failed: %v", err)
	}
	defer sqlIndex.Close()

	// Open BadgerDB cache
	queryCache, err = cache.NewCache("cache.db")
	if err != nil {
		log.Fatalf("Cache open failed: %v", err)
	}
	defer queryCache.Close()

	http.HandleFunc("/search", searchHandler)
	http.HandleFunc("/exports/", downloadHandler)

	fmt.Println("Log Search Engine running on :8080")
	fmt.Println("POST /search        — search logs")
	fmt.Println("GET  /exports/*.zip — download results")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
