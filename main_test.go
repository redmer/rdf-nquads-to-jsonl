package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// TestLongLines verifies handling of lines exceeding traditional buffer limits
func TestLongLines(t *testing.T) {
	tests := []struct {
		name       string
		lineLength int
	}{
		{"1KB line", 1024},
		{"100KB line", 100 * 1024},
		{"1MB line", 1024 * 1024},
		{"5MB line", 5 * 1024 * 1024},
		{"10MB line", 10 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a very long literal value
			longValue := strings.Repeat("x", tt.lineLength-200) // Reserve space for RDF structure

			input := `<http://example.org/s> <http://example.org/p> "` + longValue + `" .` + "\n"

			// Redirect stdin
			oldStdin := os.Stdin
			r, w, _ := os.Pipe()
			os.Stdin = r

			go func() {
				w.Write([]byte(input))
				w.Close()
			}()

			// Capture stdout
			oldStdout := os.Stdout
			rOut, wOut, _ := os.Pipe()
			os.Stdout = wOut

			var buf bytes.Buffer
			outDone := make(chan struct{})
			go func() {
				buf.ReadFrom(rOut)
				close(outDone)
			}()

			// Run main logic
			if err := RunProcessor(nil, nil); err != nil {
				t.Fatalf("Processing failed: %v", err)
			}

			wOut.Close()
			<-outDone
			os.Stdin = oldStdin
			os.Stdout = oldStdout

			// Verify output stream parses as JSON
			var doc map[string]interface{}
			if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
				t.Errorf("Invalid JSON output for %s: %v", tt.name, err)
			}
		})
	}
}

// TestMixedLineLengths verifies correct processing of mixed normal/long lines and final line behavior
func TestMixedLineLengths(t *testing.T) {
	input := `<http://ex.org/1> <http://ex.org/p> "short" .
<http://ex.org/2> <http://ex.org/p> "` + strings.Repeat("x", 2*1024*1024) + `" .
<http://ex.org/3> <http://ex.org/p> "normal" .` // Note: no trailing newline here

	// Redirect stdin
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r

	go func() {
		w.Write([]byte(input))
		w.Close()
	}()

	// Capture stdout
	oldStdout := os.Stdout
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	var buf bytes.Buffer
	outDone := make(chan struct{})
	go func() {
		buf.ReadFrom(rOut)
		close(outDone)
	}()

	if err := RunProcessor(nil, nil); err != nil {
		t.Fatalf("Processing failed: %v", err)
	}

	wOut.Close()
	<-outDone
	os.Stdin = oldStdin
	os.Stdout = oldStdout
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")

	if len(lines) != 3 {
		t.Errorf("Expected 3 JSON documents, got %d", len(lines))
	}
}

func TestFiltering(t *testing.T) {
	input := `
<http://s1> <http://p> "o" <http://g1> .
<http://s2> <http://p> "o" <http://g2> .
<http://s3> <http://p> "o" <http://g3> .
<http://s4> <http://p> "o" .
`

	tests := []struct {
		name     string
		includes map[string]bool
		excludes map[string]bool
		wantIDs  []string
	}{
		{
			name:     "No filters",
			includes: nil,
			excludes: nil,
			wantIDs:  []string{"http://s1", "http://s2", "http://s3", "http://s4"},
		},
		{
			name:     "Include g1",
			includes: map[string]bool{"http://g1": true},
			excludes: nil,
			wantIDs:  []string{"http://s1"},
		},
		{
			name:     "Exclude g2",
			includes: nil,
			excludes: map[string]bool{"http://g2": true},
			wantIDs:  []string{"http://s1", "http://s3", "http://s4"},
		},
		{
			name:     "Include g1 and g3, Exclude g3",
			includes: map[string]bool{"http://g1": true, "http://g3": true},
			excludes: map[string]bool{"http://g3": true},
			wantIDs:  []string{"http://s1"},
		},
		{
			name:     "Include empty graph (default graph)",
			includes: map[string]bool{"": true},
			excludes: nil,
			wantIDs:  []string{"http://s4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
// Redirect stdin
oldStdin := os.Stdin
r, w, _ := os.Pipe()
			os.Stdin = r
			go func() {
				w.Write([]byte(input))
				w.Close()
			}()

			// Capture stdout
			oldStdout := os.Stdout
			rOut, wOut, _ := os.Pipe()
			os.Stdout = wOut

			var buf bytes.Buffer
			outDone := make(chan struct{})
			go func() {
				_, _ = buf.ReadFrom(rOut)
				close(outDone)
			}()

			if err := RunProcessor(tt.includes, tt.excludes); err != nil {
				t.Fatalf("Processing failed: %v", err)
			}

			wOut.Close()
			<-outDone
			os.Stdin = oldStdin
			os.Stdout = oldStdout

			// Parse output
			var gotIDs []string
			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			for _, line := range lines {
				if line == "" {
					continue
				}
				var doc struct {
					ID string `json:"_id"`
				}
				if err := json.Unmarshal([]byte(line), &doc); err != nil {
					t.Errorf("Invalid JSON: %v", err)
				}
				gotIDs = append(gotIDs, doc.ID)
			}

			// Verify results
			if len(gotIDs) != len(tt.wantIDs) {
				t.Errorf("Expected %d docs, got %d. Got: %v", len(tt.wantIDs), len(gotIDs), gotIDs)
			} else {
				for i, id := range gotIDs {
					if id != tt.wantIDs[i] {
						t.Errorf("Doc %d: expected %s, got %s", i, tt.wantIDs[i], id)
					}
				}
			}
		})
	}
}
