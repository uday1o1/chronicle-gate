package engine

import (
	"context"
	"errors"
	"testing"

	cruntime "github.com/uday1o1/chronicle-gate/internal/runtime"
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

func TestCanceledOperationIsInterrupted(t *testing.T) {
	t.Parallel()
	if classification := classifyOperationalError(context.Canceled); classification != "INTERRUPTED" {
		t.Fatalf("classification = %q", classification)
	}
}

func TestPartialEnvironmentCleanupFailureOverridesCancellation(t *testing.T) {
	t.Parallel()
	err := errors.Join(context.Canceled, &cruntime.CleanupError{Err: errors.New("network removal failed")})
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

func TestViolationsUseDeclaredPrecedenceAndStableKeys(t *testing.T) {
	t.Parallel()
	attempts := []AttemptEvidence{
		{Signature: &FailureSignature{Digest: "semantic", Classification: "SEMANTIC_REGRESSION", ObservationID: "z", Pointer: "/z", Expected: 1, Actual: 2}},
		{Signature: &FailureSignature{Digest: "external", Classification: "EXTERNAL_EFFECT_REGRESSION", ObservationID: "a", Pointer: "/a", Expected: "x", Actual: "y"}},
		{Signature: &FailureSignature{Digest: "schema", Classification: "SCHEMA_REGRESSION", ObservationID: "m", Pointer: "/m", Expected: true, Actual: false}},
	}
	violations := violationsFromAttempts(attempts)
	if len(violations) != 3 || violations[0].Classification != "SCHEMA_REGRESSION" || violations[1].Classification != "EXTERNAL_EFFECT_REGRESSION" || violations[2].Classification != "SEMANTIC_REGRESSION" {
		t.Fatalf("violation order = %#v", violations)
	}
}
