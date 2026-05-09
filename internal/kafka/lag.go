package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// PartitionLag holds offset information for a single partition.
type PartitionLag struct {
	Partition       int   `json:"partition"`
	LastOffset      int64 `json:"last_offset"`
	CommittedOffset int64 `json:"committed_offset"`
	Lag             int64 `json:"lag"`
}

// LagReport holds the full lag report for a topic/group.
type LagReport struct {
	Topic      string         `json:"topic"`
	Group      string         `json:"group"`
	Partitions []PartitionLag `json:"partitions"`
	TotalLag   int64          `json:"total_lag"`
}

// LagChecker queries Kafka for consumer group lag.
type LagChecker struct {
	client  *kafka.Client
	topic   string
	groupID string
}

// NewLagChecker creates a LagChecker for the given broker, topic, and group.
func NewLagChecker(broker, topic, groupID string) *LagChecker {
	return &LagChecker{
		client: &kafka.Client{
			Addr:    kafka.TCP(broker),
			Timeout: 10 * time.Second,
		},
		topic:   topic,
		groupID: groupID,
	}
}

// ReadLag fetches the current consumer group lag from Kafka.
func (l *LagChecker) ReadLag(ctx context.Context) (LagReport, error) {
	// Step 1: Discover partitions via metadata.
	meta, err := l.client.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{l.topic},
	})
	if err != nil {
		return LagReport{}, fmt.Errorf("kafka lag: metadata: %w", err)
	}

	var partitionIDs []int
	for _, t := range meta.Topics {
		if t.Name == l.topic {
			if t.Error != nil {
				return LagReport{}, fmt.Errorf("kafka lag: topic metadata error: %w", t.Error)
			}
			for _, p := range t.Partitions {
				partitionIDs = append(partitionIDs, p.ID)
			}
			break
		}
	}

	if len(partitionIDs) == 0 {
		return LagReport{}, fmt.Errorf("kafka lag: topic %q not found or has no partitions", l.topic)
	}

	// Step 2: Get log-end offsets for all partitions.
	offsetRequests := make([]kafka.OffsetRequest, len(partitionIDs))
	for i, id := range partitionIDs {
		offsetRequests[i] = kafka.LastOffsetOf(id)
	}

	listResp, err := l.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			l.topic: offsetRequests,
		},
	})
	if err != nil {
		return LagReport{}, fmt.Errorf("kafka lag: list offsets: %w", err)
	}

	// Step 3: Get committed offsets for the consumer group.
	fetchResp, err := l.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: l.groupID,
		Topics: map[string][]int{
			l.topic: partitionIDs,
		},
	})
	if err != nil {
		return LagReport{}, fmt.Errorf("kafka lag: offset fetch: %w", err)
	}
	if fetchResp.Error != nil {
		return LagReport{}, fmt.Errorf("kafka lag: offset fetch response: %w", fetchResp.Error)
	}

	// Step 4: Build lag report.
	lastOffsets := indexPartitionOffsets(listResp.Topics[l.topic])
	committed := indexFetchPartitions(fetchResp.Topics[l.topic])

	report := LagReport{
		Topic: l.topic,
		Group: l.groupID,
	}

	for _, id := range partitionIDs {
		lastOffset := lastOffsets[id]
		committedOffset := committed[id]
		if committedOffset < 0 {
			committedOffset = 0
		}
		lag := lastOffset - committedOffset
		if lag < 0 {
			lag = 0
		}
		report.Partitions = append(report.Partitions, PartitionLag{
			Partition:       id,
			LastOffset:      lastOffset,
			CommittedOffset: committedOffset,
			Lag:             lag,
		})
		report.TotalLag += lag
	}

	return report, nil
}

// indexPartitionOffsets builds a map of partition ID → last offset.
func indexPartitionOffsets(partitions []kafka.PartitionOffsets) map[int]int64 {
	m := make(map[int]int64, len(partitions))
	for _, p := range partitions {
		m[p.Partition] = p.LastOffset
	}
	return m
}

// indexFetchPartitions builds a map of partition ID → committed offset.
func indexFetchPartitions(partitions []kafka.OffsetFetchPartition) map[int]int64 {
	m := make(map[int]int64, len(partitions))
	for _, p := range partitions {
		m[p.Partition] = p.CommittedOffset
	}
	return m
}
