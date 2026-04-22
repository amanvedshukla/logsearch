package types

type Query struct {
	Keywords        []string `json:"keywords"`
	ExcludeKeywords []string `json:"exclude_keywords"`
	RegexPattern    string   `json:"regex_pattern"`
	CaseSensitive   bool     `json:"case_sensitive"`
	StartTime       string   `json:"start_time"`
	EndTime         string   `json:"end_time"`
	Mode            string   `json:"mode"`
	RequestID       string   `json:"request_id"`
	FromLine        int      `json:"from_line"`
	ToLine          int      `json:"to_line"`
	SearchPath      string   `json:"search_path"`
	ContextLines    int      `json:"context_lines"`
	SearchOrder     string   `json:"search_order"`
	MaxLinesPerFile int      `json:"max_lines_per_file"`
	MaxMatches      int64    `json:"max_matches"`
}

type Match struct {
	FilePath     string   `json:"file_path"`
	LineNumber   int      `json:"line_number"`
	LineText     string   `json:"line_text"`
	MatchedTerms []string `json:"matched_terms"`
	ContextLines []string `json:"context_lines,omitempty"`
	ContextStart int      `json:"context_start,omitempty"`
}

type SearchResult struct {
	RequestID     string  `json:"request_id"`
	Matches       []Match `json:"matches"`
	TotalFiles    int     `json:"total_files"`
	FilesSearched int     `json:"files_searched"`
	CacheHit      bool    `json:"cache_hit"`
}
