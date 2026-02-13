package main

import (
	"bufio"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

// EscapeResult holds one escape analysis annotation from the Go compiler.
type EscapeResult struct {
	RelFile string
	Line    int
	Col     int
	Kind    string // "leaking_param", "moved_to_heap", "does_not_escape", "inlineable"
	Detail  string // variable or function name
}

// RunEscapeAnalysis runs `go build -gcflags=-m` on each module directory
// and parses the compiler's escape analysis decisions.
func RunEscapeAnalysis(prog *Progress) []EscapeResult {
	prog.Log("Running Go escape analysis (-gcflags=-m) across %d modules...", len(modSet.Dirs()))

	var allResults []EscapeResult

	for _, mod := range modSet.Dirs() {
		results := runEscapeForDir(mod.Dir, mod.Prefix, prog)
		allResults = append(allResults, results...)
	}

	prog.Log("Escape analysis: %d annotations total", len(allResults))
	return allResults
}

func runEscapeForDir(dir, prefix string, prog *Progress) []EscapeResult {
	cmd := exec.Command("go", "build", "-gcflags=-m", "./...")
	cmd.Dir = dir
	cmd.Env = replaceEnv(os.Environ(), "GOFLAGS", "-buildvcs=false")
	cmd.Stdout = nil // discard

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		prog.Verbose("Escape analysis for %s: failed to create stderr pipe: %v", dir, err)
		return nil
	}
	if err := cmd.Start(); err != nil {
		prog.Verbose("Escape analysis for %s: failed to start: %v", dir, err)
		return nil
	}

	lineRe := regexp.MustCompile(`^(?:\./)?([^:]+):(\d+):(\d+): (.+)$`)

	var results []EscapeResult
	scanner := bufio.NewScanner(stderrPipe)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		text := scanner.Text()
		if strings.HasPrefix(text, "#") || strings.HasPrefix(text, "/") {
			continue
		}
		m := lineRe.FindStringSubmatch(text)
		if m == nil {
			continue
		}

		file := m[1]
		line, _ := strconv.Atoi(m[2])
		col, _ := strconv.Atoi(m[3])
		msg := m[4]

		var kind, detail string
		switch {
		case strings.Contains(msg, "leaking param:"):
			kind = "leaking_param"
			if idx := strings.Index(msg, "leaking param:"); idx >= 0 {
				detail = strings.TrimSpace(msg[idx+len("leaking param:"):])
			}
		case strings.Contains(msg, "moved to heap:"):
			kind = "moved_to_heap"
			if idx := strings.Index(msg, "moved to heap:"); idx >= 0 {
				detail = strings.TrimSpace(msg[idx+len("moved to heap:"):])
			}
		case strings.Contains(msg, "escapes to heap"):
			kind = "escapes_to_heap"
			detail = strings.TrimSuffix(strings.TrimSpace(msg), " escapes to heap")
		case strings.Contains(msg, "does not escape"):
			kind = "does_not_escape"
			detail = strings.TrimSuffix(strings.TrimSpace(msg), " does not escape")
		case strings.HasPrefix(msg, "can inline "):
			kind = "inlineable"
			detail = strings.TrimPrefix(msg, "can inline ")
		default:
			continue
		}

		// Prefix the file path for non-primary modules
		relFile := file
		if prefix != "" {
			relFile = prefix + "/" + file
		}

		results = append(results, EscapeResult{
			RelFile: relFile,
			Line:    line,
			Col:     col,
			Kind:    kind,
			Detail:  detail,
		})
	}

	_ = cmd.Wait()
	return results
}
