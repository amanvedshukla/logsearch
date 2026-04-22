package filter

import (
	"compress/gzip"
	"os"
	"strings"

	"github.com/bits-and-blooms/bloom/v3"
)

const bloomFalsePositiveRate = 0.01 // 1% false positive rate
const bloomExpectedItems = 10000    // expected unique words per file

// BuildBloomForFile — read a .gz file and add all words to a bloom filter
func BuildBloomForFile(filePath string) (*bloom.BloomFilter, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	bf := bloom.NewWithEstimates(bloomExpectedItems, bloomFalsePositiveRate)

	// Read entire file, add every word to bloom filter
	buf := make([]byte, 1024*1024) // 1MB read buffer
	var leftover strings.Builder

	for {
		n, err := gz.Read(buf)
		if n > 0 {
			chunk := leftover.String() + string(buf[:n])
			leftover.Reset()

			lines := strings.Split(chunk, "\n")
			// Last element might be incomplete line — save for next iteration
			for i, line := range lines {
				if i == len(lines)-1 {
					leftover.WriteString(line)
				} else {
					// Add each word in the line to bloom filter
					for _, word := range strings.Fields(line) {
						bf.Add([]byte(strings.ToLower(word)))
					}
				}
			}
		}
		if err != nil {
			break
		}
	}

	// Handle any remaining content
	if leftover.Len() > 0 {
		for _, word := range strings.Fields(leftover.String()) {
			bf.Add([]byte(strings.ToLower(word)))
		}
	}

	return bf, nil
}

// BloomCheck — returns true if file MIGHT contain ALL keywords (OR: any keyword)
// Returns false if file DEFINITELY does not contain the keyword
func BloomCheck(bf *bloom.BloomFilter, keywords []string, mode string) bool {
	if mode == "AND" {
		// AND mode: all keywords must potentially be present
		for _, kw := range keywords {
			if !bf.Test([]byte(strings.ToLower(kw))) {
				return false // definitely missing one keyword — skip file
			}
		}
		return true
	}

	// OR mode: at least one keyword might be present
	for _, kw := range keywords {
		if bf.Test([]byte(strings.ToLower(kw))) {
			return true
		}
	}
	return false
}
