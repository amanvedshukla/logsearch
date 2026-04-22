package search

import (
	"testing"

	"logsearch/internal/types"
)

// Test 1: Basic OR search — find lines with ERROR
func TestSearchFile_OR(t *testing.T) {
	trie := BuildTrie([]string{"ERROR"})
	matches := SearchFile("../../testdata/app-2026-04-07-10.log.gz", trie, []string{"ERROR"}, "OR", 0, 0)

	if len(matches) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(matches))
	}

	// Verify line numbers
	if matches[0].LineNumber != 2 {
		t.Errorf("Expected first match on line 2, got line %d", matches[0].LineNumber)
	}
	if matches[1].LineNumber != 4 {
		t.Errorf("Expected second match on line 4, got line %d", matches[1].LineNumber)
	}
}

// Test 2: AND search — line must have BOTH keywords
func TestSearchFile_AND(t *testing.T) {
	trie := BuildTrie([]string{"ERROR", "timeout"})
	matches := SearchFile("../../testdata/app-2026-04-07-10.log.gz", trie, []string{"ERROR", "timeout"}, "AND", 0, 0)

	// Only line 4 has both ERROR and timeout
	if len(matches) != 1 {
		t.Errorf("Expected 1 match (AND mode), got %d", len(matches))
	}
	if len(matches) > 0 && matches[0].LineNumber != 4 {
		t.Errorf("Expected match on line 4, got line %d", matches[0].LineNumber)
	}
}

// Test 3: No matches — keyword not in file
func TestSearchFile_NoMatch(t *testing.T) {
	trie := BuildTrie([]string{"CRITICAL"})
	matches := SearchFile("../../testdata/clean-2026-04-07-10.log.gz", trie, []string{"CRITICAL"}, "OR", 0, 0)

	if len(matches) != 0 {
		t.Errorf("Expected 0 matches, got %d", len(matches))
	}
}

// Test 4: Parallel search across multiple files
func TestParallelSearch(t *testing.T) {
	files := []string{
		"../../testdata/app-2026-04-07-10.log.gz",
		"../../testdata/app-2026-04-07-11.log.gz",
		"../../testdata/sys-2026-04-07-10.log.gz",
	}

	trie := BuildTrie([]string{"ERROR"})
	matches := ParallelSearch(files, trie, []string{"ERROR"}, "OR", 0, 0)

	// app-10 has 2, app-11 has 2, sys-10 has 1 = total 5
	if len(matches) != 5 {
		t.Errorf("Expected 5 total matches across 3 files, got %d", len(matches))
	}
}

// Test 5: MatchedTerms are correctly populated
func TestMatchedTerms(t *testing.T) {
	trie := BuildTrie([]string{"ERROR"})
	matches := SearchFile("../../testdata/app-2026-04-07-10.log.gz", trie, []string{"ERROR"}, "OR", 0, 0)

	for _, m := range matches {
		found := false
		for _, term := range m.MatchedTerms {
			if term == "ERROR" {
				found = true
			}
		}
		if !found {
			t.Errorf("Expected 'ERROR' in MatchedTerms, got %v", m.MatchedTerms)
		}
		// Verify types.Match fields are populated
		if m.FilePath == "" {
			t.Error("FilePath should not be empty")
		}
		if m.LineText == "" {
			t.Error("LineText should not be empty")
		}
		_ = types.Match{}
	}
}
