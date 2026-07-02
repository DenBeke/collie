package eventstore_test

import (
	"context"
	"fmt"

	"eventstore"
	"eventstore/memory"
)

const (
	courseCapacity       = 2 // seats per course
	maxCoursesPerStudent = 2 // courses a student may take
)

// SubscribeStudentToCourse is the canonical DCB example: one command whose
// decision spans three consistency boundaries, each a differently-scoped
// projection.
//
// The two hot boundaries (a course accumulates many subscriptions, so does a
// student) are PartitionedProjections: per-course and per-student snapshots
// that catch up from their own checkpoint instead of replaying history. The
// student+course pair sees at most one event ever, so a snapshot would cost
// more than it saves — it stays an ephemeral DefaultProjection.
type SubscribeStudentToCourse struct {
	StudentID string `json:"studentId"`
	CourseID  string `json:"courseId"`

	snapshots eventstore.SnapshotStore

	courseSeats    *eventstore.PartitionedProjection[int]
	studentCourses *eventstore.PartitionedProjection[int]
	subscribed     *eventstore.DefaultProjection[bool]
}

func (cmd *SubscribeStudentToCourse) Projections(ctx context.Context) ([]eventstore.Projection, error) {
	count := func(count int, _ eventstore.SequencedEvent) (int, error) { return count + 1, nil }

	// Partition {course: X}: seats taken in this course.
	courseSeats, err := eventstore.NewPartitionedProjection(ctx, cmd.snapshots, "course-seats",
		eventstore.NewTags("course", cmd.CourseID),
		eventstore.DefaultProjection[int]{
			Query:    eventstore.QueryItem{Types: []string{"StudentSubscribed"}},
			Handlers: map[string]eventstore.HandleFunc[int]{"StudentSubscribed": count},
		})
	if err != nil {
		return nil, fmt.Errorf("could not load course seats: %w", err)
	}

	// Partition {student: X}: courses this student takes.
	studentCourses, err := eventstore.NewPartitionedProjection(ctx, cmd.snapshots, "student-courses",
		eventstore.NewTags("student", cmd.StudentID),
		eventstore.DefaultProjection[int]{
			Query:    eventstore.QueryItem{Types: []string{"StudentSubscribed"}},
			Handlers: map[string]eventstore.HandleFunc[int]{"StudentSubscribed": count},
		})
	if err != nil {
		return nil, fmt.Errorf("could not load student courses: %w", err)
	}

	// Pair {student: X, course: Y}: already subscribed? Ephemeral on purpose.
	subscribed := eventstore.DefaultProjection[bool]{
		Query: eventstore.QueryItem{
			Types: []string{"StudentSubscribed"},
			Tags:  eventstore.NewTags("student", cmd.StudentID, "course", cmd.CourseID),
		},
		Handlers: map[string]eventstore.HandleFunc[bool]{
			"StudentSubscribed": func(bool, eventstore.SequencedEvent) (bool, error) { return true, nil },
		},
	}

	cmd.courseSeats = courseSeats
	cmd.studentCourses = studentCourses
	cmd.subscribed = &subscribed
	return []eventstore.Projection{cmd.courseSeats, cmd.studentCourses, cmd.subscribed}, nil
}

func (cmd *SubscribeStudentToCourse) Decide(context.Context) ([]eventstore.Event, error) {
	if cmd.subscribed.State {
		return nil, fmt.Errorf("student %s is already subscribed to %s", cmd.StudentID, cmd.CourseID)
	}
	if cmd.courseSeats.State >= courseCapacity {
		return nil, fmt.Errorf("course %s is full", cmd.CourseID)
	}
	if cmd.studentCourses.State >= maxCoursesPerStudent {
		return nil, fmt.Errorf("student %s already takes %d courses", cmd.StudentID, maxCoursesPerStudent)
	}
	// One event, two boundaries — this is why tags are a list, not a map.
	event, err := eventstore.NewEvent("StudentSubscribed", nil,
		eventstore.NewTags("student", cmd.StudentID, "course", cmd.CourseID))
	if err != nil {
		return nil, fmt.Errorf("could not create StudentSubscribed event: %w", err)
	}
	return []eventstore.Event{event}, nil
}

// ExampleNewPartitionedProjection subscribes students to courses. Every rule is
// enforced against its own boundary: course capacity against the course
// partition, the per-student limit against the student partition, and
// duplicates against the ephemeral pair projection — while each append stays
// guarded by a condition covering exactly those three queries.
func ExampleNewPartitionedProjection() {
	ctx := context.Background()
	store := memory.New()
	snapshots := memory.NewSnapshotStore()

	subscribe := func(student, course string) {
		err := eventstore.Execute(ctx, store, &SubscribeStudentToCourse{
			StudentID: student,
			CourseID:  course,
			snapshots: snapshots,
		})
		fmt.Printf("%s -> %s: %v\n", student, course, err)
	}

	subscribe("s1", "math")      // ok
	subscribe("s1", "math")      // duplicate
	subscribe("s2", "math")      // ok — course now full
	subscribe("s3", "math")      // course full
	subscribe("s1", "physics")   // ok — student now at limit
	subscribe("s1", "chemistry") // student limit

	// Output:
	// s1 -> math: <nil>
	// s1 -> math: student s1 is already subscribed to math
	// s2 -> math: <nil>
	// s3 -> math: course math is full
	// s1 -> physics: <nil>
	// s1 -> chemistry: student s1 already takes 2 courses
}
