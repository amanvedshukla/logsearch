package filter

import (
	"log"

	"logsearch/internal/index"
)

// FilterPipeline — runs all 3 filters and returns candidate files
// Stage 1: Time Range (O1) → Stage 2: Bloom Filter → Stage 3: SQLite Index
func FilterPipeline(
	logsRoot string,
	startTime string,
	endTime string,
	keywords []string,
	mode string,
	sqlIndex *index.SQLiteIndex,
) ([]string, error) {

	// ── STAGE 1: Time Range Filter ──────────────────────────────
	timeFiles, err := GetFilesInTimeRange(logsRoot, startTime, endTime)
	if err != nil {
		return nil, err
	}
	log.Printf("[FILTER] Stage 1 Time: %d files in time range", len(timeFiles))

	if len(timeFiles) == 0 {
		return nil, nil
	}

	// ── STAGE 2: Bloom Filter ───────────────────────────────────
	var bloomCandidates []string
	for _, filePath := range timeFiles {
		bf, err := BuildBloomForFile(filePath)
		if err != nil {
			log.Printf("[FILTER] Bloom build error for %s: %v", filePath, err)
			continue
		}
		if BloomCheck(bf, keywords, mode) {
			bloomCandidates = append(bloomCandidates, filePath)
		}
	}
	log.Printf("[FILTER] Stage 2 Bloom: %d files remaining (skipped %d)",
		len(bloomCandidates), len(timeFiles)-len(bloomCandidates))

	if len(bloomCandidates) == 0 {
		return nil, nil
	}

	// ── STAGE 3: SQLite Index ───────────────────────────────────
	if sqlIndex == nil {
		// No index available — return bloom candidates as-is
		log.Printf("[FILTER] Stage 3 SQLite: skipped (no index)")
		return bloomCandidates, nil
	}

	sqlFiles, err := sqlIndex.QueryFilesForKeywords(keywords, mode)
	if err != nil {
		log.Printf("[FILTER] SQLite query error: %v — using bloom candidates", err)
		return bloomCandidates, nil
	}

	// Intersect bloom candidates with SQLite results
	sqlSet := make(map[string]bool)
	for _, f := range sqlFiles {
		sqlSet[f] = true
	}

	var finalCandidates []string
	for _, f := range bloomCandidates {
		if sqlSet[f] {
			finalCandidates = append(finalCandidates, f)
		}
	}

	log.Printf("[FILTER] Stage 3 SQLite: %d final candidates (skipped %d)",
		len(finalCandidates), len(bloomCandidates)-len(finalCandidates))

	return finalCandidates, nil
}
