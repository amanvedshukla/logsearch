package search

import (
	"bufio"
	"compress/gzip"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	ahocorasick "github.com/BobuSumisu/aho-corasick"
	"logsearch/internal/types"
)

// Progress — live stats during search
type Progress struct {
	FilesTotal     int64
	FilesSearched  int64
	LinesScanned   int64
	MatchesFound   int64
	BytesProcessed int64
}

// ChunkedSearch — search file in chunks, report progress live via channel
func ChunkedSearch(
	files []string,
	trie *ahocorasick.Trie,
	keywords []string,
	mode string,
	fromLine, toLine int,
	chunkSize int, // lines per chunk
	progressCh chan<- Progress,
) []types.Match {

	var (
		totalFiles    = int64(len(files))
		filesSearched int64
		linesScanned  int64
		matchesFound  int64
		bytesProc     int64
	)

	resultsCh := make(chan []types.Match, len(files))
	var wg sync.WaitGroup

	for _, file := range files {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			defer func() {
				atomic.AddInt64(&filesSearched, 1)
				// Send progress update
				if progressCh != nil {
					progressCh <- Progress{
						FilesTotal:     totalFiles,
						FilesSearched:  atomic.LoadInt64(&filesSearched),
						LinesScanned:   atomic.LoadInt64(&linesScanned),
						MatchesFound:   atomic.LoadInt64(&matchesFound),
						BytesProcessed: atomic.LoadInt64(&bytesProc),
					}
				}
			}()

			info, _ := os.Stat(f)
			if info != nil {
				atomic.AddInt64(&bytesProc, info.Size())
			}

			fp, err := os.Open(f)
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

			var matches []types.Match
			scanner := bufio.NewScanner(gz)
			scanner.Buffer(*bufPtr, len(*bufPtr))

			lineNum := 0
			chunkCount := 0

			for scanner.Scan() {
				lineNum++
				chunkCount++

				if fromLine > 0 && lineNum < fromLine {
					continue
				}
				if toLine > 0 && lineNum > toLine {
					break
				}

				atomic.AddInt64(&linesScanned, 1)

				// Progress update every chunkSize lines
				if chunkCount >= chunkSize {
					chunkCount = 0
					if progressCh != nil {
						progressCh <- Progress{
							FilesTotal:     totalFiles,
							FilesSearched:  atomic.LoadInt64(&filesSearched),
							LinesScanned:   atomic.LoadInt64(&linesScanned),
							MatchesFound:   atomic.LoadInt64(&matchesFound),
							BytesProcessed: atomic.LoadInt64(&bytesProc),
						}
					}
				}

				line := scanner.Text()
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

				atomic.AddInt64(&matchesFound, 1)
				matches = append(matches, types.Match{
					FilePath:     f,
					LineNumber:   lineNum,
					LineText:     line,
					MatchedTerms: foundTerms,
				})
			}

			resultsCh <- matches
		}(file)
	}

	wg.Wait()
	close(resultsCh)

	var allMatches []types.Match
	for m := range resultsCh {
		allMatches = append(allMatches, m...)
	}
	return allMatches
}
