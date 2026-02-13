package main

import (
	"fmt"
	"os"
	"time"
)

// Progress reports pipeline progress to stderr with elapsed time.
type Progress struct {
	start   time.Time
	verbose bool
}

// NewProgress creates a progress reporter.
func NewProgress(verbose bool) *Progress {
	return &Progress{start: time.Now(), verbose: verbose}
}

// Log prints a progress message with elapsed time prefix.
func (p *Progress) Log(format string, args ...any) {
	elapsed := time.Since(p.start)
	mins := int(elapsed.Minutes())
	secs := int(elapsed.Seconds()) % 60
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "[%02d:%02d] %s\n", mins, secs, msg)
}

// Verbose prints only when verbose mode is enabled.
func (p *Progress) Verbose(format string, args ...any) {
	if p.verbose {
		p.Log(format, args...)
	}
}
