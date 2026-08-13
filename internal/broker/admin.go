package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// RecordIdentity is the physical Kafka record identity required for redelivery evidence.
type RecordIdentity struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
	EventHash string `json:"eventSha256"`
}

// RewindEvidence proves the old, requested, and verified broker positions.
type RewindEvidence struct {
	LogStart       int64 `json:"logStart"`
	LogEnd         int64 `json:"logEnd"`
	OldCommitted   int64 `json:"oldCommitted"`
	Requested      int64 `json:"requested"`
	Verified       int64 `json:"verified"`
	FinalCommitted int64 `json:"finalCommitted"`
}

// Admin owns host-side Kafka clients for one run.
type Admin struct {
	client *kgo.Client
	kadm   *kadm.Client
}

func NewAdmin(brokers ...string) (*Admin, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("create Kafka admin client: %w", err)
	}
	return &Admin{client: client, kadm: kadm.NewClient(client)}, nil
}

func (admin *Admin) Close() {
	admin.client.Close()
}

func (admin *Admin) CreateTopic(ctx context.Context, topic string) error {
	if _, err := admin.kadm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}
	return nil
}

func (admin *Admin) DeleteTopic(ctx context.Context, topic string) error {
	response, err := admin.kadm.DeleteTopic(ctx, topic)
	if err != nil {
		return fmt.Errorf("delete topic %q: %w", topic, err)
	}
	if response.Err != nil {
		return fmt.Errorf("delete topic %q: %w", topic, response.Err)
	}
	return nil
}

func (admin *Admin) Publish(ctx context.Context, topic string, partition int32, key, value []byte, eventHash string) (RecordIdentity, error) {
	record := &kgo.Record{Topic: topic, Partition: partition, Key: key, Value: value}
	result := admin.client.ProduceSync(ctx, record)
	if err := result.FirstErr(); err != nil {
		return RecordIdentity{}, fmt.Errorf("publish Kafka record: %w", err)
	}
	return RecordIdentity{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Key: string(record.Key), EventHash: eventHash}, nil
}

func (admin *Admin) CommittedOffset(ctx context.Context, group, topic string, partition int32) (int64, error) {
	responses, err := admin.kadm.FetchOffsets(ctx, group)
	if err != nil {
		return 0, fmt.Errorf("fetch committed offsets for %q: %w", group, err)
	}
	response, exists := responses.Lookup(topic, partition)
	if !exists || response.Err != nil || response.At < 0 {
		if exists && response.Err != nil {
			return 0, fmt.Errorf("committed offset for %s[%d]: %w", topic, partition, response.Err)
		}
		return 0, fmt.Errorf("committed offset for %s[%d] is absent", topic, partition)
	}
	return response.At, nil
}

func (admin *Admin) WaitCommitted(ctx context.Context, group, topic string, partition int32, expected int64) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		position, err := admin.CommittedOffset(ctx, group, topic, partition)
		if err == nil && position == expected {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for committed offset %d: %w", expected, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (admin *Admin) RequireGroupMember(ctx context.Context, group, clientID string) error {
	described, err := admin.kadm.DescribeGroups(ctx, group)
	if err != nil {
		return fmt.Errorf("describe group %q: %w", group, err)
	}
	description, exists := described[group]
	if !exists || description.Err != nil {
		return fmt.Errorf("group %q is unavailable", group)
	}
	if len(description.Members) != 1 || description.Members[0].ClientID != clientID {
		return fmt.Errorf("group %q must contain exactly client %q; members=%d", group, clientID, len(description.Members))
	}
	return nil
}

func (admin *Admin) WaitGroupEmpty(ctx context.Context, group string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		described, err := admin.kadm.DescribeGroups(ctx, group)
		if err == nil {
			description, exists := described[group]
			if exists && description.Err == nil && len(description.Members) == 0 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for group %q to become empty: %w", group, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (admin *Admin) Rewind(ctx context.Context, group, topic string, partition int32, requested int64) (RewindEvidence, error) {
	described, err := admin.kadm.DescribeGroups(ctx, group)
	if err != nil {
		return RewindEvidence{}, fmt.Errorf("describe group before rewind: %w", err)
	}
	if groupDescription, exists := described[group]; exists && len(groupDescription.Members) != 0 {
		return RewindEvidence{}, fmt.Errorf("refuse offset rewind while group %q has %d members", group, len(groupDescription.Members))
	}
	starts, err := admin.kadm.ListStartOffsets(ctx, topic)
	if err != nil {
		return RewindEvidence{}, fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := admin.kadm.ListEndOffsets(ctx, topic)
	if err != nil {
		return RewindEvidence{}, fmt.Errorf("list end offsets: %w", err)
	}
	start, startExists := starts.Lookup(topic, partition)
	end, endExists := ends.Lookup(topic, partition)
	if !startExists || !endExists || start.Err != nil || end.Err != nil {
		return RewindEvidence{}, fmt.Errorf("offset bounds for %s[%d] are unavailable", topic, partition)
	}
	old, err := admin.CommittedOffset(ctx, group, topic, partition)
	if err != nil {
		return RewindEvidence{}, err
	}
	if err := ValidateRewindBounds(requested, start.Offset, end.Offset); err != nil {
		return RewindEvidence{}, err
	}
	offsets := kadm.Offsets{topic: {partition: {Topic: topic, Partition: partition, At: requested, LeaderEpoch: -1}}}
	if err := admin.kadm.CommitAllOffsets(ctx, group, offsets); err != nil {
		return RewindEvidence{}, fmt.Errorf("commit rewind offset: %w", err)
	}
	verified, err := admin.CommittedOffset(ctx, group, topic, partition)
	if err != nil {
		return RewindEvidence{}, fmt.Errorf("verify rewind offset: %w", err)
	}
	if verified != requested {
		return RewindEvidence{}, fmt.Errorf("rewind verification mismatch: requested %d, broker returned %d", requested, verified)
	}
	return RewindEvidence{LogStart: start.Offset, LogEnd: end.Offset, OldCommitted: old, Requested: requested, Verified: verified}, nil
}

func ValidateRewindBounds(requested, start, end int64) error {
	if start < 0 || end < start {
		return fmt.Errorf("invalid broker offset bounds [%d,%d)", start, end)
	}
	if requested < start || requested >= end {
		return fmt.Errorf("requested rewind offset %d is outside broker bounds [%d,%d)", requested, start, end)
	}
	return nil
}
