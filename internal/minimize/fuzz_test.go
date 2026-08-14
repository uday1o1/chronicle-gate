package minimize

import (
	"fmt"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func FuzzReducerDependencyClosure(f *testing.F) {
	f.Add([]byte{1, 0, 3, 2, 7, 4})
	f.Fuzz(func(t *testing.T, shape []byte) {
		if len(shape) == 0 || len(shape) > 32 {
			t.Skip()
		}
		scenario := spec.Scenario{Spec: spec.ScenarioSpec{Events: map[string]spec.CloudEvent{}}}
		for index, value := range shape {
			id := fmt.Sprintf("step-%d", index)
			event := fmt.Sprintf("event-%d", index)
			step := spec.Step{ID: id, Optional: value&1 == 1, Publish: &spec.PublishAction{Event: event}}
			if index > 0 && value&2 == 2 {
				step.DependsOn = []string{fmt.Sprintf("step-%d", int(value)%index)}
			}
			scenario.Spec.Events[event] = spec.CloudEvent{ID: event}
			scenario.Spec.Steps = append(scenario.Spec.Steps, step)
		}
		for _, proposal := range HierarchicalProposals(scenario) {
			steps := map[string]struct{}{}
			for _, step := range proposal.Scenario.Spec.Steps {
				steps[step.ID] = struct{}{}
			}
			for _, step := range proposal.Scenario.Spec.Steps {
				for _, dependency := range step.DependsOn {
					if _, exists := steps[dependency]; !exists {
						t.Fatalf("proposal %q leaves unresolved dependency %q", proposal.Description, dependency)
					}
				}
				if step.Publish != nil {
					if _, exists := proposal.Scenario.Spec.Events[step.Publish.Event]; !exists {
						t.Fatalf("proposal %q leaves unresolved event %q", proposal.Description, step.Publish.Event)
					}
				}
			}
		}
	})
}
