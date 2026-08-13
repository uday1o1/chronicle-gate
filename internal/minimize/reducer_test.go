package minimize

import (
	"context"
	"testing"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func TestReducerPreservesDependencyClosureAndOrder(t *testing.T) {
	scenario := spec.Scenario{Spec: spec.ScenarioSpec{
		Events: map[string]spec.CloudEvent{"noise": {}, "required": {}},
		Steps: []spec.Step{
			{ID: "noise", Optional: true, Publish: &spec.PublishAction{Event: "noise"}},
			{ID: "noise-child", DependsOn: []string{"noise"}, Observe: &spec.ObserveAction{}},
			{ID: "required", Publish: &spec.PublishAction{Event: "required"}},
		},
	}}
	reducer := Reducer{MaxTrials: 10, Deadline: time.Now().Add(time.Minute)}
	result, summary := reducer.Reduce(context.Background(), scenario, func(_ context.Context, proposal spec.Scenario) (Outcome, string) {
		return SameFailure, ""
	})
	if len(result.Spec.Steps) != 1 || result.Spec.Steps[0].ID != "required" || len(result.Spec.Events) != 1 {
		t.Fatalf("invalid reduced closure: %#v", result.Spec)
	}
	if summary.Minimality != "proven" || summary.FinalActions != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestReducerNeverAcceptsUnresolved(t *testing.T) {
	scenario := spec.Scenario{Spec: spec.ScenarioSpec{Events: map[string]spec.CloudEvent{"noise": {}}, Steps: []spec.Step{{ID: "noise", Optional: true, Publish: &spec.PublishAction{Event: "noise"}}}}}
	result, summary := (Reducer{MaxTrials: 1, Deadline: time.Now().Add(time.Minute)}).Reduce(context.Background(), scenario, func(context.Context, spec.Scenario) (Outcome, string) {
		return Unresolved, "infrastructure"
	})
	if len(result.Spec.Steps) != 1 || summary.Minimality != "not_proven" {
		t.Fatalf("unresolved proposal was accepted: %#v %#v", result, summary)
	}
}

func TestReducerReportsTrialBudgetExhaustion(t *testing.T) {
	scenario := spec.Scenario{Spec: spec.ScenarioSpec{Events: map[string]spec.CloudEvent{"a": {}, "b": {}}, Steps: []spec.Step{
		{ID: "a", Optional: true, Publish: &spec.PublishAction{Event: "a"}},
		{ID: "b", Optional: true, Publish: &spec.PublishAction{Event: "b"}},
	}}}
	_, summary := (Reducer{MaxTrials: 1, Deadline: time.Now().Add(time.Minute)}).Reduce(context.Background(), scenario, func(context.Context, spec.Scenario) (Outcome, string) {
		return Pass, "does not preserve signature"
	})
	if summary.Status != "budget_exhausted" || summary.Minimality != "not_proven" || summary.Trials != 1 {
		t.Fatalf("summary = %#v", summary)
	}
}
