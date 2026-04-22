package search

import (
	"bufio"
	"compress/gzip"
	"os"
	"strings"
	"sync"
	"time"

	ahocorasick "github.com/BobuSumisu/aho-corasick"
	"logsearch/internal/types"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 4*1024*1024) // 4MB
		return &buf
	},
}

func BuildTrie(keywords []string) *ahocorasick.Trie {
	builder := ahocorasick.NewTrieBuilder()
	for _, kw := range keywords {
		builder.AddPattern([]byte(strings.ToLower(kw)))
	}
	return builder.Build()
}

// SearchFile with optional line range and time filter (empty = no filter)
func SearchFile(path string, trie *ahocorasick.Trie, keywords []string, mode string, fromLine, toLine int) []types.Match {
	return SearchFileWithTime(path, trie, keywords, mode, fromLine, toLine, "", "")
}

func SearchFileWithTime(path string, trie *ahocorasick.Trie, keywords []string, mode string, fromLine, toLine int, startTime, endTime string) []types.Match {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil
	}
	defer gz.Close()

	bufPtr := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufPtr)

	var matches []types.Match
	scanner := bufio.NewScanner(gz)
	scanner.Buffer(*bufPtr, len(*bufPtr))

	lineNum := 0
	for scanner.Scan() {
		lineNum++

		// Line range filter — skip lines outside range
		if fromLine > 0 && lineNum < fromLine {
			continue
		}
		if toLine > 0 && lineNum > toLine {
			break // no need to read further
		}

		line := scanner.Text()
		// Time filter — parse timestamp from line if time range set
		if startTime != "" || endTime != "" {
			if len(line) >= 19 {
				ts := line[:19] // "2006-01-02 15:04:05"
				if t, err := time.Parse("2006-01-02 15:04:05", ts); err == nil {
					if startTime != "" {
						if st, err2 := time.Parse("2006-01-02 15:04", startTime); err2 == nil && t.Before(st) {
							continue
						}
					}
					if endTime != "" {
						if et, err2 := time.Parse("2006-01-02 15:04", endTime); err2 == nil && t.After(et) {
							continue
						}
					}
				}
			}
		}
		lower := strings.ToLower(line)
		hits := trie.MatchString(lower)
		if len(hits) == 0 {
			continue
		}

		foundMap := make(map[string]bool)
		for _, hit := range hits {
			foundMap[string(hit.Match())] = true
		}

		var foundTerms []string
		for _, kw := range keywords {
			if foundMap[strings.ToLower(kw)] {
				foundTerms = append(foundTerms, kw)
			}
		}

		if mode == "AND" && len(foundTerms) != len(keywords) {
			continue
		}
		if mode == "OR" && len(foundTerms) == 0 {
			continue
		}

		matches = append(matches, types.Match{
			FilePath:     path,
			LineNumber:   lineNum,
			LineText:     line,
			MatchedTerms: foundTerms,
		})
	}

	return matches
}

func ParallelSearch(files []string, trie *ahocorasick.Trie, keywords []string, mode string, fromLine, toLine int) []types.Match {
	return ParallelSearchWithTime(files, trie, keywords, mode, fromLine, toLine, "", "")
}

func ParallelSearchWithTime(files []string, trie *ahocorasick.Trie, keywords []string, mode string, fromLine, toLine int, startTime, endTime string) []types.Match {
	resultsChan := make(chan []types.Match, len(files))
	var wg sync.WaitGroup

	for _, file := range files {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			matches := SearchFileWithTime(f, trie, keywords, mode, fromLine, toLine, startTime, endTime)
			resultsChan <- matches
		}(file)
	}

	wg.Wait()
	close(resultsChan)

	var allMatches []types.Match
	for matches := range resultsChan {
		allMatches = append(allMatches, matches...)
	}
	return allMatches
}
