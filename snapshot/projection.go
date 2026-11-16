package snapshot

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/DenBeke/collie"
)

// Projection is an [collie.CheckpointProjection] whose state and position
// are persisted through a [Store], so it can resume from its last checkpoint
// instead of replaying the whole stream. Apply only updates the in-memory
// state; the snapshot is written by Checkpoint, which BuildDecisionModel and
// UpdateProjection call once per catch-up.
//
// The state type S is marshaled to and from JSON here — in the constructor and
// Checkpoint — so the Store only ever sees the resulting bytes.
type Projection[S any] struct {
	collie.DefaultProjection[S]
	store    Store
	name     string
	position uint64
	dirty    bool
}

// Compile-time check that Projection implements CheckpointProjection.
var _ collie.CheckpointProjection = (*Projection[any])(nil)

// NewProjection creates a snapshot-backed projection under name. If a snapshot
// exists, its state and position are restored; otherwise the projection starts
// from def's initial state at position 0.
//
// Partition tags scope the projection to one consistency boundary (one room,
// one student, one student+course pair): they are added to def's query (AND
// semantics), and the snapshot name is derived from name plus the sorted,
// escaped tags — so the same partition always resolves to the same snapshot
// regardless of tag order, distinct partitions can never collide, and each
// partition keeps its own independent checkpoint. With no partition tags the
// projection is the singleton under name.
//
// A snapshot is only valid for the handler logic that built it — when handlers
// change, version the name (e.g. "course-seats/v2") so old snapshots are
// abandoned and rebuilt instead of silently read as wrong state.
func NewProjection[S any](ctx context.Context, store Store, name string, def collie.DefaultProjection[S], partition ...collie.Tag) (*Projection[S], error) {
	def.Query.Tags = mergeTags(def.Query.Tags, partition)

	p := &Projection[S]{
		DefaultProjection: def,
		store:             store,
		name:              snapshotName(name, partition),
	}
	state, position, found, err := store.Load(ctx, p.name)
	if err != nil {
		return nil, fmt.Errorf("could not load projection %q: %w", p.name, err)
	}
	if found {
		p.position = position
		if len(state) > 0 {
			if err := json.Unmarshal(state, &p.State); err != nil {
				return nil, fmt.Errorf("could not unmarshal projection %q state: %w", p.name, err)
			}
		}
	}
	return p, nil
}

// After returns the sequence number of the last applied event.
func (p *Projection[S]) After() uint64 {
	return p.position
}

// Apply applies a single event to the in-memory state and advances the
// position. The snapshot is not persisted until Checkpoint.
func (p *Projection[S]) Apply(ctx context.Context, e collie.SequencedEvent) error {
	if err := p.DefaultProjection.Apply(ctx, e); err != nil {
		return fmt.Errorf("could not apply %s event: %w", e.Type, err)
	}
	p.position = e.SequenceNumber
	p.dirty = true
	return nil
}

// checkpointGap is how far the head may move past a saved snapshot before an
// unchanged snapshot is saved again to record it.
const checkpointGap = 10_000

// Checkpoint records the catch-up's progress by persisting the in-memory state
// and position through the store. It is a no-op when nothing was applied and
// upTo is less than checkpointGap past the position.
func (p *Projection[S]) Checkpoint(ctx context.Context, upTo uint64) error {
	if !p.dirty && upTo < p.position+checkpointGap {
		return nil
	}
	p.position = max(p.position, upTo)
	state, err := json.Marshal(p.State)
	if err != nil {
		return fmt.Errorf("could not marshal projection %q state: %w", p.name, err)
	}
	if err := p.store.Save(ctx, p.name, state, p.position); err != nil {
		return fmt.Errorf("could not save projection %q: %w", p.name, err)
	}
	p.dirty = false
	return nil
}

// Reset deletes the persisted snapshot and resets the in-memory state and
// position to their zero values.
func (p *Projection[S]) Reset(ctx context.Context) error {
	if err := p.store.Delete(ctx, p.name); err != nil {
		return fmt.Errorf("could not reset projection %q: %w", p.name, err)
	}
	var zero S
	p.State = zero
	p.position = 0
	p.dirty = false
	return nil
}

// mergeTags appends the tags of extra not already present in base.
func mergeTags(base, extra []collie.Tag) []collie.Tag {
	merged := slices.Clone(base)
	for _, tag := range extra {
		if !slices.Contains(merged, tag) {
			merged = append(merged, tag)
		}
	}
	return merged
}

// snapshotName builds the canonical snapshot name: the escaped projection name
// followed by the partition tags sorted by key then value. Sorting makes the
// name order-insensitive; escaping the separator characters makes it
// collision-free (a tag value containing '|' or '=' cannot masquerade as a
// different tag set, and a name containing them cannot masquerade as a
// partition).
func snapshotName(name string, partition []collie.Tag) string {
	tags := slices.Clone(partition)
	slices.SortFunc(tags, func(a, b collie.Tag) int {
		if c := cmp.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return cmp.Compare(a.Value, b.Value)
	})

	var b strings.Builder
	b.WriteString(escapePart(name))
	for _, tag := range tags {
		b.WriteByte('|')
		b.WriteString(escapePart(tag.Key))
		b.WriteByte('=')
		b.WriteString(escapePart(tag.Value))
	}
	return b.String()
}

// escapePart percent-encodes the characters snapshotName uses as structure
// ('|', '=') and '%' itself.
func escapePart(s string) string {
	return strings.NewReplacer("%", "%25", "|", "%7C", "=", "%3D").Replace(s)
}
