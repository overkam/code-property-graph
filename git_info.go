package main

import (
	"bufio"
	"os/exec"
	"strconv"
	"strings"
)

// GitFileHistory holds per-file git change metrics.
type GitFileHistory struct {
	RelFile       string
	CommitCount   int
	AuthorCount   int
	LastAuthor    string
	LastDate      string // ISO 8601
	Insertions    int
	Deletions     int
	DaysSinceEdit int
}

// GitBlameEntry holds per-line-range blame data from git blame --porcelain.
type GitBlameEntry struct {
	RelFile string
	Line    int
	Author  string
	Date    string // ISO 8601
	Commit  string // short SHA
}

// RunGitHistory extracts per-file change frequency from `git log --numstat`
// across all modules in the ModuleSet.
func RunGitHistory(prog *Progress) []GitFileHistory {
	prog.Log("Running git log for file history across %d modules...", len(modSet.Dirs()))

	var allResults []GitFileHistory

	for _, mod := range modSet.Dirs() {
		results := runGitHistoryForDir(mod.Dir, mod.Prefix, prog)
		allResults = append(allResults, results...)
	}

	prog.Log("Git history: %d files with change data", len(allResults))
	return allResults
}

func runGitHistoryForDir(dir, prefix string, prog *Progress) []GitFileHistory {
	cmd := exec.Command("git", "log", "--format=%H %aI %aN", "--numstat", "--no-merges", "-n", "500")
	cmd.Dir = dir

	out, err := cmd.Output()
	if err != nil {
		prog.Verbose("Git history for %s: failed: %v", dir, err)
		return nil
	}

	type fileStats struct {
		commits    map[string]bool
		authors    map[string]bool
		lastAuthor string
		lastDate   string
		ins, del   int
	}
	files := make(map[string]*fileStats)

	var currentAuthor, currentDate string
	var currentCommit string

	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Commit header: "abc123 2024-01-01T00:00:00+00:00 Author Name"
		if len(line) > 40 && line[40] == ' ' {
			parts := strings.SplitN(line, " ", 3)
			if len(parts) == 3 {
				currentCommit = parts[0][:12]
				currentDate = parts[1]
				currentAuthor = parts[2]
			}
			continue
		}

		// Numstat line: "123\t456\tpath/to/file.go"
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) != 3 {
			continue
		}
		ins, err1 := strconv.Atoi(parts[0])
		del, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil {
			continue // binary file
		}
		relFile := parts[2]
		if !strings.HasSuffix(relFile, ".go") {
			continue
		}

		// Prefix for non-primary modules
		if prefix != "" {
			relFile = prefix + "/" + relFile
		}

		fs, ok := files[relFile]
		if !ok {
			fs = &fileStats{
				commits: make(map[string]bool),
				authors: make(map[string]bool),
			}
			files[relFile] = fs
		}
		fs.commits[currentCommit] = true
		fs.authors[currentAuthor] = true
		fs.ins += ins
		fs.del += del
		// First commit encountered is most recent (git log is newest-first)
		if fs.lastAuthor == "" {
			fs.lastAuthor = currentAuthor
			fs.lastDate = currentDate
		}
	}

	var results []GitFileHistory
	for file, fs := range files {
		results = append(results, GitFileHistory{
			RelFile:     file,
			CommitCount: len(fs.commits),
			AuthorCount: len(fs.authors),
			LastAuthor:  fs.lastAuthor,
			LastDate:    fs.lastDate,
			Insertions:  fs.ins,
			Deletions:   fs.del,
		})
	}

	return results
}

// RunGitBlame extracts per-function blame data using `git blame --porcelain`.
// Only samples function declaration lines to keep the data manageable.
func RunGitBlame(dir string, files []string, prog *Progress) []GitBlameEntry {
	prog.Log("Running git blame for %d files...", len(files))

	var results []GitBlameEntry
	for _, relFile := range files {
		cmd := exec.Command("git", "blame", "--porcelain", "--", relFile)
		cmd.Dir = dir

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			continue
		}
		if err := cmd.Start(); err != nil {
			continue
		}

		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

		var currentLine int
		var currentAuthor, currentDate, currentCommit string

		for scanner.Scan() {
			text := scanner.Text()

			// Header line: "commit_sha orig_line final_line [num_lines]"
			if len(text) >= 40 && text[0] != '\t' && !strings.HasPrefix(text, "author") &&
				!strings.HasPrefix(text, "committer") && !strings.HasPrefix(text, "summary") &&
				!strings.HasPrefix(text, "previous") && !strings.HasPrefix(text, "filename") &&
				!strings.HasPrefix(text, "boundary") {
				parts := strings.Fields(text)
				if len(parts) >= 3 {
					currentCommit = parts[0][:12]
					currentLine, _ = strconv.Atoi(parts[2])
				}
			} else if strings.HasPrefix(text, "author ") {
				currentAuthor = strings.TrimPrefix(text, "author ")
			} else if strings.HasPrefix(text, "author-time ") {
				currentDate = strings.TrimPrefix(text, "author-time ")
			} else if len(text) > 0 && text[0] == '\t' {
				// Content line — emit entry
				results = append(results, GitBlameEntry{
					RelFile: relFile,
					Line:    currentLine,
					Author:  currentAuthor,
					Date:    currentDate,
					Commit:  currentCommit,
				})
			}
		}

		_ = cmd.Wait()
	}

	prog.Log("Git blame: %d line entries across %d files", len(results), len(files))
	return results
}
