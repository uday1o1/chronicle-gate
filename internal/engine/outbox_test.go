package engine

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/effects"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func TestOutboxPlansAcceptCrashAndControlCorpus(t *testing.T) {
	t.Parallel()
	baseline := loadOutboxTarget(t, "r7-baseline.yaml")
	candidate := loadOutboxTarget(t, "r7-candidate.yaml")
	for _, name := range []string{"r7-outbox-crash-after-ack.yaml", "r7-unrelated-orders-control.yaml"} {
		t.Run(name, func(t *testing.T) {
			scenario := loadOutboxScenario(t, name)
			for _, target := range []spec.Target{baseline, candidate} {
				plan, err := buildOutboxPlan(scenario, target)
				if err != nil {
					t.Fatal(err)
				}
				if plan.trigger.Action.Topic != "outbox-relay-trigger" || len(plan.observations) != len(scenario.Spec.Observations) || !isOutboxScenario(scenario) {
					t.Fatalf("outbox plan is incomplete: %#v", plan)
				}
			}
			if violations := spec.CompareTargets(baseline, candidate, scenario.Spec.Comparison.AllowedTargetDifferences); len(violations) != 0 {
				t.Fatalf("R7 target comparison violations: %#v", violations)
			}
		})
	}
}

func TestOutboxPlanFailsClosedOnPartialCrashShape(t *testing.T) {
	t.Parallel()
	scenario := loadOutboxScenario(t, "r7-outbox-crash-after-ack.yaml")
	for index := range scenario.Spec.Steps {
		if scenario.Spec.Steps[index].ID == "restart-relay" {
			scenario.Spec.Steps[index].Restart.Service = "order-workflow"
		}
	}
	if _, err := buildOutboxPlan(scenario, loadOutboxTarget(t, "r7-baseline.yaml")); err == nil || !strings.Contains(err.Error(), "restart only") {
		t.Fatalf("partial crash shape error = %v", err)
	}
}

func TestOutboxEffectDifferenceUsesExternalEffectSignature(t *testing.T) {
	t.Parallel()
	plan := outboxPlan{observations: []plannedObservation{}}
	baseline := AttemptEvidence{EffectProjection: []effects.SemanticEntry{{Kind: "payment_capture", BusinessKey: "order-1", Amount: 100, IdempotencyKey: "event-1"}}}
	candidate := AttemptEvidence{EffectProjection: append(append([]effects.SemanticEntry{}, baseline.EffectProjection...), effects.SemanticEntry{Kind: "payment_capture", BusinessKey: "order-1", Amount: 100, IdempotencyKey: "event-2"})}
	signature, err := compareOutboxAttempts(baseline, candidate, plan)
	if err != nil {
		t.Fatal(err)
	}
	if signature == nil || signature.Classification != "EXTERNAL_EFFECT_REGRESSION" || signature.Pointer != "/entries/count" || signature.Expected != 1 || signature.Actual != 2 {
		t.Fatalf("unexpected outbox signature: %#v", signature)
	}
}

func loadOutboxScenario(t *testing.T, name string) spec.Scenario {
	t.Helper()
	scenario, err := spec.LoadScenario(filepath.Join("..", "..", "examples", "order-lifecycle", "scenarios", name))
	if err != nil {
		t.Fatal(err)
	}
	return scenario
}

func loadOutboxTarget(t *testing.T, name string) spec.Target {
	t.Helper()
	target, err := spec.LoadTarget(filepath.Join("..", "..", "examples", "order-lifecycle", "targets", name))
	if err != nil {
		t.Fatal(err)
	}
	return target
}
