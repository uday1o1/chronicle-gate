package engine

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func TestControlledPlansAcceptTheM6Corpus(t *testing.T) {
	t.Parallel()
	target := loadControlledTarget(t)
	for _, name := range []string{
		"r3-stale-aggregate-overwrite.yaml",
		"r3-monotonic-version-control.yaml",
		"r5-payment-first-control.yaml",
		"r5-inventory-first-regression.yaml",
		"r6-late-cancellation.yaml",
		"r6-on-time-cancellation-control.yaml",
	} {
		t.Run(name, func(t *testing.T) {
			scenario := loadControlledScenario(t, name)
			plan, err := buildControlledPlan(scenario, target)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.publication) != 2 || len(plan.runtime.Streams) == 0 || !isControlledScenario(scenario) {
				t.Fatalf("controlled plan is incomplete: %#v", plan)
			}
		})
	}
}

func TestControlledPlansFailClosedOnUnsupportedSchedules(t *testing.T) {
	t.Parallel()
	target := loadControlledTarget(t)
	t.Run("same topic cross stream", func(t *testing.T) {
		scenario := loadControlledScenario(t, "r5-payment-first-control.yaml")
		for index := range scenario.Spec.Steps {
			if scenario.Spec.Steps[index].Publish != nil && scenario.Spec.Steps[index].Publish.Topic == "inventory-events" {
				scenario.Spec.Steps[index].Publish.Topic = "payment-events"
			}
		}
		if _, err := buildControlledPlan(scenario, target); err == nil || !strings.Contains(err.Error(), "distinct logical topics") {
			t.Fatalf("same-topic R5 error = %v", err)
		}
	})
	t.Run("release before both blocked", func(t *testing.T) {
		scenario := loadControlledScenario(t, "r5-payment-first-control.yaml")
		for index := range scenario.Spec.Steps {
			if scenario.Spec.Steps[index].ID == "release-payment" {
				scenario.Spec.Steps[index].DependsOn = []string{"await-payment"}
			}
		}
		if _, err := buildControlledPlan(scenario, target); err == nil || !strings.Contains(err.Error(), "both handlers blocked") {
			t.Fatalf("early release error = %v", err)
		}
	})
	t.Run("authored late event is on time", func(t *testing.T) {
		scenario := loadControlledScenario(t, "r6-late-cancellation.yaml")
		event := scenario.Spec.Events["cancellation"]
		event.Time = "2026-08-13T14:00:00Z"
		scenario.Spec.Events["cancellation"] = event
		if _, err := buildControlledPlan(scenario, target); err == nil || !strings.Contains(err.Error(), "expectLate") {
			t.Fatalf("false lateness error = %v", err)
		}
	})
}

func loadControlledScenario(t *testing.T, name string) spec.Scenario {
	t.Helper()
	path := filepath.Join("..", "..", "examples", "order-lifecycle", "scenarios", name)
	scenario, err := spec.LoadScenario(path)
	if err != nil {
		t.Fatal(err)
	}
	return scenario
}

func loadControlledTarget(t *testing.T) spec.Target {
	t.Helper()
	target, err := spec.LoadTarget(filepath.Join("..", "..", "examples", "order-lifecycle", "targets", "state-baseline.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return target
}
