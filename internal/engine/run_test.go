package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func TestFailureSignatureIsDeterministic(t *testing.T) {
	t.Parallel()
	invariant := spec.Invariant{ID: "no-duplicates", Classification: "SEMANTIC_REGRESSION"}
	left, err := NewFailureSignature(invariant, []map[string]any{{"sku": "sku-1", "reservation_count": int64(2), "order_id": "order-1"}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewFailureSignature(invariant, []map[string]any{{"order_id": "order-1", "reservation_count": int64(2), "sku": "sku-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if left.Digest != right.Digest {
		t.Fatalf("signature digest changed with map order: %s != %s", left.Digest, right.Digest)
	}
}

func TestInfrastructureCleanupFailureOverridesTimeout(t *testing.T) {
	t.Parallel()
	err := joinInfrastructure(context.DeadlineExceeded, errors.New("cleanup failed"))
	if classification := classifyOperationalError(err); classification != "INFRASTRUCTURE_ERROR" {
		t.Fatalf("classification = %q", classification)
	}
}

func TestClassifyCandidateRequiresMatchingConfirmation(t *testing.T) {
	t.Parallel()
	signature := &FailureSignature{Digest: "same"}
	classification, _ := classifyCandidate([]AttemptEvidence{{Signature: signature}, {Signature: signature}, {Signature: signature}})
	if classification != "SEMANTIC_REGRESSION" {
		t.Fatalf("classification = %q", classification)
	}
	classification, _ = classifyCandidate([]AttemptEvidence{{Signature: signature}, {}, {Signature: signature}})
	if classification != "FLAKY" {
		t.Fatalf("mixed classification = %q", classification)
	}
}
