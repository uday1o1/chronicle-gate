// Command release_check enforces ChronicleGate's tracked portfolio-release policy.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/uday1o1/chronicle-gate/internal/evidence"
)

func main() {
	repository := flag.String("repository", ".", "repository root")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "positional arguments are forbidden")
		os.Exit(2)
	}
	if err := evidence.CheckReleaseRepository(*repository); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("repository release policy: PASS")
}
