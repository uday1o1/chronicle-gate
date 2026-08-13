// Package minimize performs bounded, dependency-safe scenario reduction.
package minimize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

type Outcome string

const (
	Pass        Outcome = "PASS"
	SameFailure Outcome = "SAME_FAILURE"
	Unresolved  Outcome = "UNRESOLVED"
)

type Predicate func(context.Context, spec.Scenario) (Outcome, string)

type Proposal struct {
	Scenario    spec.Scenario
	Description string
}

type Rejection struct {
	Transform string  `json:"transform"`
	Outcome   Outcome `json:"outcome"`
	Reason    string  `json:"reason"`
}

type Summary struct {
	Status             string      `json:"status"`
	Minimality         string      `json:"minimality"`
	OriginalEvents     int         `json:"originalEvents"`
	FinalEvents        int         `json:"finalEvents"`
	OriginalActions    int         `json:"originalActions"`
	FinalActions       int         `json:"finalActions"`
	Trials             int         `json:"trials"`
	CacheHits          int         `json:"cacheHits"`
	AcceptedTransforms []string    `json:"acceptedTransforms"`
	Rejections         []Rejection `json:"rejections"`
}

type Reducer struct {
	MaxTrials int
	Deadline  time.Time
	CacheKey  string
}

func (reducer Reducer) Reduce(ctx context.Context, original spec.Scenario, predicate Predicate) (spec.Scenario, Summary) {
	current := clone(original)
	summary := Summary{
		Status: "complete", Minimality: "not_proven", OriginalEvents: len(original.Spec.Events),
		OriginalActions: len(original.Spec.Steps), AcceptedTransforms: []string{}, Rejections: []Rejection{},
	}
	cache := map[string]struct {
		outcome Outcome
		reason  string
	}{}
	budgetExhausted := false
	unresolvedSeen := false

	for {
		accepted := false
		for _, proposal := range HierarchicalProposals(current) {
			if reducer.MaxTrials > 0 && summary.Trials >= reducer.MaxTrials || !reducer.Deadline.IsZero() && time.Now().After(reducer.Deadline) {
				budgetExhausted = true
				break
			}
			hash, err := CanonicalHash(proposal.Scenario)
			if err != nil {
				summary.Rejections = append(summary.Rejections, Rejection{Transform: proposal.Description, Outcome: Unresolved, Reason: err.Error()})
				unresolvedSeen = true
				continue
			}
			key := reducer.CacheKey + ":" + hash
			cached, exists := cache[key]
			if exists {
				summary.CacheHits++
			} else {
				summary.Trials++
				cached.outcome, cached.reason = predicate(ctx, proposal.Scenario)
				cache[key] = cached
			}
			if cached.outcome == SameFailure {
				current = proposal.Scenario
				summary.AcceptedTransforms = append(summary.AcceptedTransforms, proposal.Description)
				accepted = true
				break
			}
			if cached.outcome == Unresolved {
				unresolvedSeen = true
			}
			summary.Rejections = append(summary.Rejections, Rejection{Transform: proposal.Description, Outcome: cached.outcome, Reason: cached.reason})
		}
		if budgetExhausted || !accepted {
			break
		}
	}

	summary.FinalEvents = len(current.Spec.Events)
	summary.FinalActions = len(current.Spec.Steps)
	if !budgetExhausted && !unresolvedSeen {
		summary.Minimality = "proven"
	}
	if budgetExhausted {
		summary.Status = "budget_exhausted"
	}
	return current, summary
}

// HierarchicalProposals returns dependency-safe transformations in the V1 reduction order.
func HierarchicalProposals(scenario spec.Scenario) []Proposal {
	proposals := []Proposal{}
	proposals = append(proposals, OptionalStepProposals(scenario)...)
	proposals = append(proposals, optionalChunkProposals(scenario)...)
	proposals = append(proposals, optionalFaultProposals(scenario)...)
	proposals = append(proposals, optionalEventFieldProposals(scenario)...)
	proposals = append(proposals, scalarBoundaryProposals(scenario)...)
	seen := map[string]struct{}{}
	unique := make([]Proposal, 0, len(proposals))
	for _, proposal := range proposals {
		hash, err := CanonicalHash(proposal.Scenario)
		if err != nil {
			continue
		}
		if _, exists := seen[hash]; exists {
			continue
		}
		seen[hash] = struct{}{}
		unique = append(unique, proposal)
	}
	return unique
}

// OptionalStepProposals removes one optional step plus its dependent closure.
func OptionalStepProposals(scenario spec.Scenario) []Proposal {
	proposals := []Proposal{}
	for _, step := range scenario.Spec.Steps {
		if !step.Optional {
			continue
		}
		removed := map[string]struct{}{step.ID: {}}
		changed := true
		for changed {
			changed = false
			for _, candidate := range scenario.Spec.Steps {
				if _, exists := removed[candidate.ID]; exists {
					continue
				}
				for _, dependency := range candidate.DependsOn {
					if _, exists := removed[dependency]; exists {
						removed[candidate.ID] = struct{}{}
						changed = true
						break
					}
				}
			}
		}
		proposal := clone(scenario)
		proposal.Spec.Steps = proposal.Spec.Steps[:0]
		for _, candidate := range scenario.Spec.Steps {
			if _, remove := removed[candidate.ID]; !remove {
				proposal.Spec.Steps = append(proposal.Spec.Steps, candidate)
			}
		}
		pruneEvents(&proposal)
		proposals = append(proposals, Proposal{Scenario: proposal, Description: fmt.Sprintf("remove optional closure rooted at %s", step.ID)})
	}
	return proposals
}

func optionalChunkProposals(scenario spec.Scenario) []Proposal {
	proposals := []Proposal{}
	for start := 0; start < len(scenario.Spec.Steps); {
		if !scenario.Spec.Steps[start].Optional {
			start++
			continue
		}
		end := start
		for end+1 < len(scenario.Spec.Steps) && scenario.Spec.Steps[end+1].Optional {
			end++
		}
		if end > start {
			removed := map[string]struct{}{}
			for index := start; index <= end; index++ {
				removed[scenario.Spec.Steps[index].ID] = struct{}{}
			}
			proposal := removeClosure(scenario, removed)
			proposals = append(proposals, Proposal{Scenario: proposal, Description: fmt.Sprintf("remove optional topological chunk %s..%s", scenario.Spec.Steps[start].ID, scenario.Spec.Steps[end].ID)})
		}
		start = end + 1
	}
	return proposals
}

func optionalFaultProposals(scenario spec.Scenario) []Proposal {
	proposals := []Proposal{}
	for _, step := range scenario.Spec.Steps {
		if !step.Optional || step.RewindOffset == nil && step.Stop == nil && step.ArmCheckpoint == nil && step.ReleaseCheckpoint == nil {
			continue
		}
		proposal := removeClosure(scenario, map[string]struct{}{step.ID: {}})
		proposals = append(proposals, Proposal{Scenario: proposal, Description: "remove optional fault " + step.ID})
	}
	return proposals
}

func optionalEventFieldProposals(scenario spec.Scenario) []Proposal {
	names := sortedEventNames(scenario)
	proposals := []Proposal{}
	for _, name := range names {
		keys := sortedMapKeys(scenario.Spec.Events[name].Data)
		for _, key := range keys {
			proposal := clone(scenario)
			delete(proposal.Spec.Events[name].Data, key)
			proposals = append(proposals, Proposal{Scenario: proposal, Description: fmt.Sprintf("remove event field %s.data.%s", name, key)})
		}
	}
	return proposals
}

func scalarBoundaryProposals(scenario spec.Scenario) []Proposal {
	proposals := []Proposal{}
	for _, name := range sortedEventNames(scenario) {
		for _, key := range sortedMapKeys(scenario.Spec.Events[name].Data) {
			value := scenario.Spec.Events[name].Data[key]
			boundaries := []any{}
			switch typed := value.(type) {
			case string:
				if typed != "" {
					boundaries = []any{""}
				}
			case float64:
				boundaries = numericBoundaries(typed)
			case int:
				boundaries = numericBoundaries(float64(typed))
			case int64:
				boundaries = numericBoundaries(float64(typed))
			}
			for _, boundary := range boundaries {
				proposal := clone(scenario)
				proposal.Spec.Events[name].Data[key] = boundary
				proposals = append(proposals, Proposal{Scenario: proposal, Description: fmt.Sprintf("replace %s.data.%s with boundary %v", name, key, boundary)})
			}
		}
	}
	return proposals
}

func numericBoundaries(value float64) []any {
	boundaries := []any{}
	for _, boundary := range []float64{0, 1, -1} {
		if value != boundary {
			boundaries = append(boundaries, boundary)
		}
	}
	return boundaries
}

func removeClosure(scenario spec.Scenario, removed map[string]struct{}) spec.Scenario {
	changed := true
	for changed {
		changed = false
		for _, candidate := range scenario.Spec.Steps {
			if _, exists := removed[candidate.ID]; exists {
				continue
			}
			for _, dependency := range candidate.DependsOn {
				if _, exists := removed[dependency]; exists {
					removed[candidate.ID] = struct{}{}
					changed = true
					break
				}
			}
		}
	}
	proposal := clone(scenario)
	proposal.Spec.Steps = proposal.Spec.Steps[:0]
	for _, candidate := range scenario.Spec.Steps {
		if _, remove := removed[candidate.ID]; !remove {
			proposal.Spec.Steps = append(proposal.Spec.Steps, candidate)
		}
	}
	pruneEvents(&proposal)
	return proposal
}

func sortedEventNames(scenario spec.Scenario) []string {
	names := make([]string, 0, len(scenario.Spec.Events))
	for name := range scenario.Spec.Events {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedMapKeys(value map[string]any) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func pruneEvents(scenario *spec.Scenario) {
	used := map[string]struct{}{}
	for _, step := range scenario.Spec.Steps {
		if step.Publish != nil {
			used[step.Publish.Event] = struct{}{}
		}
	}
	for name := range scenario.Spec.Events {
		if _, exists := used[name]; !exists {
			delete(scenario.Spec.Events, name)
		}
	}
}

func CanonicalHash(scenario spec.Scenario) (string, error) {
	document, err := json.Marshal(scenario)
	if err != nil {
		return "", fmt.Errorf("canonicalize scenario: %w", err)
	}
	digest := sha256.Sum256(document)
	return hex.EncodeToString(digest[:]), nil
}

func clone(scenario spec.Scenario) spec.Scenario {
	document, _ := json.Marshal(scenario)
	var cloned spec.Scenario
	_ = json.Unmarshal(document, &cloned)
	return cloned
}
