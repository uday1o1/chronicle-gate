// Command evidence_capture executes one declared release-evidence case.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/uday1o1/chronicle-gate/internal/evidence"
)

func main() {
	repository := flag.String("repository", ".", "repository root")
	binary := flag.String("binary", "bin/chronicle", "repository-relative ChronicleGate binary")
	corpusPath := flag.String("corpus", "evidence/corpus.json", "repository-relative evidence corpus")
	caseID := flag.String("case", "", "declared semantic or benchmark case ID")
	output := flag.String("out", "", "new repository-relative capture directory")
	flag.Parse()
	if *caseID == "" || *output == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "--case and --out are required and positional arguments are forbidden")
		os.Exit(2)
	}
	absoluteRepository, err := filepath.Abs(*repository)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	corpus, err := evidence.LoadCorpus(filepath.Join(absoluteRepository, filepath.FromSlash(*corpusPath)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	options := evidence.CaptureOptions{Repository: absoluteRepository, Binary: *binary, Corpus: *corpusPath, CaseID: *caseID, Output: *output}
	if _, semanticErr := evidence.FindSemanticCase(corpus, *caseID); semanticErr == nil {
		_, err = evidence.CaptureSemantic(ctx, options)
	} else if _, benchmarkErr := evidence.FindBenchmarkCase(corpus, *caseID); benchmarkErr == nil {
		_, err = evidence.CaptureBenchmark(ctx, options)
	} else {
		err = fmt.Errorf("case %q is not declared", *caseID)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("evidence capture %s: PASS\n", *caseID)
}
