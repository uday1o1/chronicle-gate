// Command evidence_publish converts verified private captures into allowlisted public evidence.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/uday1o1/chronicle-gate/internal/evidence"
)

func main() {
	repository := flag.String("repository", ".", "repository root")
	corpus := flag.String("corpus", "evidence/corpus.json", "repository-relative evidence corpus")
	capture := flag.String("capture", "", "repository-relative semantic capture root")
	aaCapture := flag.String("benchmark-aa", "", "repository-relative A/A benchmark capture root")
	slowdownCapture := flag.String("benchmark-slowdown", "", "repository-relative slowdown benchmark capture root")
	output := flag.String("out", "", "new repository-relative public evidence file")
	flag.Parse()
	if *output == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "--out is required and positional arguments are forbidden")
		os.Exit(2)
	}
	semanticMode := *capture != "" && *aaCapture == "" && *slowdownCapture == ""
	benchmarkMode := *capture == "" && *aaCapture != "" && *slowdownCapture != ""
	if !semanticMode && !benchmarkMode {
		fmt.Fprintln(os.Stderr, "provide either --capture or both benchmark capture flags")
		os.Exit(2)
	}
	var err error
	if semanticMode {
		_, err = evidence.PublishSemantic(*repository, *corpus, *capture, *output)
	} else {
		_, err = evidence.PublishBenchmark(*repository, *corpus, *aaCapture, *slowdownCapture, *output)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("public evidence %s: PASS\n", *output)
}
