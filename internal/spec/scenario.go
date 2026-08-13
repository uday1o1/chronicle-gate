package spec

type Scenario struct {
	APIVersion string       `json:"apiVersion" yaml:"apiVersion"`
	Kind       string       `json:"kind" yaml:"kind"`
	Metadata   Metadata     `json:"metadata" yaml:"metadata"`
	Spec       ScenarioSpec `json:"spec" yaml:"spec"`
}

type ScenarioSpec struct {
	Seed          map[string]any        `json:"seed" yaml:"seed"`
	Clock         ClockSpec             `json:"clock" yaml:"clock"`
	Events        map[string]CloudEvent `json:"events" yaml:"events"`
	Steps         []Step                `json:"steps" yaml:"steps"`
	Quiescence    Quiescence            `json:"quiescence" yaml:"quiescence"`
	Observations  []Observation         `json:"observations" yaml:"observations"`
	Invariants    []Invariant           `json:"invariants" yaml:"invariants"`
	Normalization []Normalization       `json:"normalization" yaml:"normalization"`
	Limits        Limits                `json:"limits" yaml:"limits"`
	Comparison    Comparison            `json:"comparison,omitempty" yaml:"comparison,omitempty"`
}

type ClockSpec struct {
	Start string `json:"start,omitempty" yaml:"start,omitempty"`
}

type Limits struct {
	MaxSteps             int      `json:"maxSteps" yaml:"maxSteps"`
	MaxEvents            int      `json:"maxEvents" yaml:"maxEvents"`
	MaxRunDuration       Duration `json:"maxRunDuration" yaml:"maxRunDuration"`
	ConfirmationAttempts int      `json:"confirmationAttempts" yaml:"confirmationAttempts"`
	MinimizationTrials   int      `json:"minimizationTrials" yaml:"minimizationTrials"`
	MinimizationDuration Duration `json:"minimizationDuration" yaml:"minimizationDuration"`
}

type Step struct {
	ID                string              `json:"id" yaml:"id"`
	Optional          bool                `json:"optional,omitempty" yaml:"optional,omitempty"`
	DependsOn         []string            `json:"dependsOn,omitempty" yaml:"dependsOn,omitempty"`
	Timeout           Duration            `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Publish           *PublishAction      `json:"publish,omitempty" yaml:"publish,omitempty"`
	Await             *AwaitAction        `json:"await,omitempty" yaml:"await,omitempty"`
	Stop              *ServiceAction      `json:"stop,omitempty" yaml:"stop,omitempty"`
	Restart           *ServiceAction      `json:"restart,omitempty" yaml:"restart,omitempty"`
	RewindOffset      *RewindOffsetAction `json:"rewindOffset,omitempty" yaml:"rewindOffset,omitempty"`
	ArmCheckpoint     *CheckpointAction   `json:"armCheckpoint,omitempty" yaml:"armCheckpoint,omitempty"`
	ReleaseCheckpoint *CheckpointAction   `json:"releaseCheckpoint,omitempty" yaml:"releaseCheckpoint,omitempty"`
	AdvanceClock      *AdvanceClockAction `json:"advanceClock,omitempty" yaml:"advanceClock,omitempty"`
	Observe           *ObserveAction      `json:"observe,omitempty" yaml:"observe,omitempty"`
}

func (step Step) ActionCount() int {
	count := 0
	for _, present := range []bool{
		step.Publish != nil,
		step.Await != nil,
		step.Stop != nil,
		step.Restart != nil,
		step.RewindOffset != nil,
		step.ArmCheckpoint != nil,
		step.ReleaseCheckpoint != nil,
		step.AdvanceClock != nil,
		step.Observe != nil,
	} {
		if present {
			count++
		}
	}
	return count
}

type PublishAction struct {
	Event     string `json:"event" yaml:"event"`
	Topic     string `json:"topic" yaml:"topic"`
	Partition int    `json:"partition" yaml:"partition"`
	Key       string `json:"key" yaml:"key"`
}

type AwaitAction struct {
	Condition  string              `json:"condition,omitempty" yaml:"condition,omitempty"`
	Checkpoint *CheckpointSelector `json:"checkpoint,omitempty" yaml:"checkpoint,omitempty"`
}

type ServiceAction struct {
	Service string `json:"service" yaml:"service"`
}

type RewindOffsetAction struct {
	Service   string `json:"service" yaml:"service"`
	Group     string `json:"group" yaml:"group"`
	Topic     string `json:"topic" yaml:"topic"`
	Partition int    `json:"partition" yaml:"partition"`
	ToOffset  int64  `json:"toOffset" yaml:"toOffset"`
}

type CheckpointSelector struct {
	Service    string `json:"service" yaml:"service"`
	Name       string `json:"name" yaml:"name"`
	EventID    string `json:"eventId" yaml:"eventId"`
	StepID     string `json:"stepId" yaml:"stepId"`
	Occurrence int    `json:"occurrence" yaml:"occurrence"`
}

type CheckpointAction struct {
	CheckpointSelector `json:",inline" yaml:",inline"`
}

type AdvanceClockAction struct {
	By Duration `json:"by" yaml:"by"`
}

type ObserveAction struct {
	Observation string `json:"observation" yaml:"observation"`
}

type Quiescence struct {
	Timeout         Duration              `json:"timeout" yaml:"timeout"`
	StabilityWindow Duration              `json:"stabilityWindow" yaml:"stabilityWindow"`
	Conditions      []QuiescenceCondition `json:"conditions" yaml:"conditions"`
}

type QuiescenceCondition struct {
	ID      string `json:"id" yaml:"id"`
	Type    string `json:"type" yaml:"type"`
	Service string `json:"service,omitempty" yaml:"service,omitempty"`
	Group   string `json:"group,omitempty" yaml:"group,omitempty"`
	State   string `json:"state,omitempty" yaml:"state,omitempty"`
}

type Observation struct {
	ID      string              `json:"id" yaml:"id"`
	SQL     *SQLObservation     `json:"sql,omitempty" yaml:"sql,omitempty"`
	Kafka   *KafkaObservation   `json:"kafka,omitempty" yaml:"kafka,omitempty"`
	HTTP    *HTTPObservation    `json:"http,omitempty" yaml:"http,omitempty"`
	Effects *EffectsObservation `json:"effects,omitempty" yaml:"effects,omitempty"`
}

func (observation Observation) TypeCount() int {
	count := 0
	for _, present := range []bool{observation.SQL != nil, observation.Kafka != nil, observation.HTTP != nil, observation.Effects != nil} {
		if present {
			count++
		}
	}
	return count
}

type SQLObservation struct {
	QueryFile string   `json:"queryFile" yaml:"queryFile"`
	OrderBy   []string `json:"orderBy" yaml:"orderBy"`
}

type KafkaObservation struct {
	Topic       string `json:"topic" yaml:"topic"`
	StartOffset int64  `json:"startOffset" yaml:"startOffset"`
	EndOffset   int64  `json:"endOffset" yaml:"endOffset"`
	Mode        string `json:"mode" yaml:"mode"`
	KeyPointer  string `json:"keyPointer,omitempty" yaml:"keyPointer,omitempty"`
}

type HTTPObservation struct {
	URL string `json:"url" yaml:"url"`
}

type EffectsObservation struct {
	URL string `json:"url" yaml:"url"`
}

type Invariant struct {
	ID             string `json:"id" yaml:"id"`
	QueryFile      string `json:"queryFile" yaml:"queryFile"`
	Classification string `json:"classification" yaml:"classification"`
}

type Normalization struct {
	ID          string   `json:"id" yaml:"id"`
	Observation string   `json:"observation" yaml:"observation"`
	Type        string   `json:"type" yaml:"type"`
	Pointer     string   `json:"pointer" yaml:"pointer"`
	Token       string   `json:"token,omitempty" yaml:"token,omitempty"`
	Keys        []string `json:"keys,omitempty" yaml:"keys,omitempty"`
	Tolerance   *float64 `json:"tolerance,omitempty" yaml:"tolerance,omitempty"`
}
