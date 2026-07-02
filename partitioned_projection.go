package eventstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// PartitionedProjection is a StoredProjection whose snapshot identity and event
// query are both derived from a partition: the set of tags identifying one
// consistency boundary (one room, one student, one student+course pair). Each
// partition is an independent snapshot with its own checkpoint, so a command
// loads exactly one entity's state — never the whole projection — and commands
// on different partitions share nothing.
//
// The partition tags are AND-ed into the definition's query, and the snapshot
// name is a canonical encoding of the projection name, version, and sorted
// partition tags — so the same partition always resolves to the same snapshot
// regardless of tag order, and distinct partitions can never collide.
type PartitionedProjection[S any] struct {
	StoredProjection[S]
	partition []Tag
}

// PartitionedOption configures a PartitionedProjection.
type PartitionedOption func(*partitionedConfig)

type partitionedConfig struct {
	version int
}

// WithVersion sets the snapshot schema version (default 1). A snapshot is only
// valid for the handler logic that built it — bump the version when handlers
// change, so existing snapshots are abandoned and rebuilt instead of silently
// read as wrong state. Values below 1 are ignored.
func WithVersion(version int) PartitionedOption {
	return func(config *partitionedConfig) {
		if version >= 1 {
			config.version = version
		}
	}
}

// NewPartitionedProjection creates the projection for one partition of name,
// backed by store. The partition must contain at least one tag; its tags are
// added to def's query (AND semantics), scoping the projection to that
// boundary. If a snapshot exists for this partition, state and position are
// restored; otherwise the projection starts from def's initial state at
// position 0.
func NewPartitionedProjection[S any](ctx context.Context, store SnapshotStore, name string, partition []Tag, def DefaultProjection[S], options ...PartitionedOption) (*PartitionedProjection[S], error) {
	if len(partition) == 0 {
		return nil, errors.New("partition must contain at least one tag")
	}
	config := partitionedConfig{version: 1}
	for _, option := range options {
		option(&config)
	}

	def.Query.Tags = mergeTags(def.Query.Tags, partition)

	inner, err := NewStoredProjection(ctx, store, partitionName(name, config.version, partition), def)
	if err != nil {
		return nil, err
	}
	return &PartitionedProjection[S]{
		StoredProjection: *inner,
		partition:        slices.Clone(partition),
	}, nil
}

// Partition returns the tags identifying this partition.
func (p *PartitionedProjection[S]) Partition() []Tag {
	return slices.Clone(p.partition)
}

// mergeTags appends the tags of extra not already present in base.
func mergeTags(base, extra []Tag) []Tag {
	merged := slices.Clone(base)
	for _, tag := range extra {
		if !slices.Contains(merged, tag) {
			merged = append(merged, tag)
		}
	}
	return merged
}

// partitionName builds the canonical snapshot name for a partition: the
// escaped projection name and version followed by the partition tags sorted by
// key then value. Sorting makes the name order-insensitive; escaping the
// separator characters makes it collision-free (a tag value containing '|' or
// '=' cannot masquerade as a different tag set).
func partitionName(name string, version int, partition []Tag) string {
	tags := slices.Clone(partition)
	slices.SortFunc(tags, func(a, b Tag) int {
		if c := cmp.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return cmp.Compare(a.Value, b.Value)
	})

	var b strings.Builder
	b.WriteString(escapePartitionPart(name))
	fmt.Fprintf(&b, "@v%d", version)
	for _, tag := range tags {
		b.WriteByte('|')
		b.WriteString(escapePartitionPart(tag.Key))
		b.WriteByte('=')
		b.WriteString(escapePartitionPart(tag.Value))
	}
	return b.String()
}

// escapePartitionPart percent-encodes the characters partitionName uses as
// structure ('|', '=', '@') and '%' itself.
func escapePartitionPart(s string) string {
	return strings.NewReplacer("%", "%25", "|", "%7C", "=", "%3D", "@", "%40").Replace(s)
}
