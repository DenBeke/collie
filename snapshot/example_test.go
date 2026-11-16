package snapshot_test

import (
	"context"
	"fmt"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/command"
	"github.com/DenBeke/collie/memory"
	"github.com/DenBeke/collie/snapshot"
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
// student) are snapshot projections partitioned by tag: per-course and
// per-student snapshots that catch up from their own checkpoint instead of
// replaying history. The student+course pair sees at most one event ever, so a
// snapshot would cost more than it saves — it stays an ephemeral
// DefaultProjection.
type SubscribeStudentToCourse struct {
	StudentID string `json:"studentId"`
	CourseID  string `json:"courseId"`

	snapshots snapshot.Store

	courseSeats    *snapshot.Projection[int]
	studentCourses *snapshot.Projection[int]
	subscribed     *collie.DefaultProjection[bool]
}

// countSubscriptions counts StudentSubscribed events; the partition tags passed
// to snapshot.NewProjection scope which ones. The event types to read derive
// from the handler keys, so no Query is spelled out.
var countSubscriptions = collie.DefaultProjection[int]{
	Handlers: map[string]collie.HandleFunc[int]{
		"StudentSubscribed": func(count int, _ collie.SequencedEvent) (int, error) { return count + 1, nil },
	},
}

func (cmd *SubscribeStudentToCourse) Projections(ctx context.Context) ([]collie.Projection, error) {
	// Partition {course: X}: seats taken in this course.
	courseSeats, err := snapshot.NewProjection(ctx, cmd.snapshots, "course-seats", countSubscriptions,
		collie.NewTag("course", cmd.CourseID))
	if err != nil {
		return nil, fmt.Errorf("could not load course seats: %w", err)
	}

	// Partition {student: X}: courses this student takes.
	studentCourses, err := snapshot.NewProjection(ctx, cmd.snapshots, "student-courses", countSubscriptions,
		collie.NewTag("student", cmd.StudentID))
	if err != nil {
		return nil, fmt.Errorf("could not load student courses: %w", err)
	}

	// Pair {student: X, course: Y}: already subscribed? Ephemeral on purpose.
	subscribed := collie.DefaultProjection[bool]{
		Query: collie.QueryItem{
			Tags: collie.NewTags("student", cmd.StudentID, "course", cmd.CourseID),
		},
		Handlers: map[string]collie.HandleFunc[bool]{
			"StudentSubscribed": func(bool, collie.SequencedEvent) (bool, error) { return true, nil },
		},
	}

	cmd.courseSeats = courseSeats
	cmd.studentCourses = studentCourses
	cmd.subscribed = &subscribed
	return []collie.Projection{cmd.courseSeats, cmd.studentCourses, cmd.subscribed}, nil
}

func (cmd *SubscribeStudentToCourse) Decide(context.Context) ([]collie.Event, error) {
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
	event, err := collie.NewEvent("StudentSubscribed", nil,
		collie.NewTags("student", cmd.StudentID, "course", cmd.CourseID))
	if err != nil {
		return nil, fmt.Errorf("could not create StudentSubscribed event: %w", err)
	}
	return []collie.Event{event}, nil
}

// ExampleNewProjection subscribes students to courses. Every rule is enforced
// against its own boundary: course capacity against the course partition, the
// per-student limit against the student partition, and duplicates against the
// ephemeral pair projection — while each append stays guarded by a condition
// covering exactly those three queries.
func ExampleNewProjection() {
	ctx := context.Background()
	store := memory.New()
	snapshots := memory.NewSnapshotStore()

	subscribe := func(student, course string) {
		err := command.Execute(ctx, store, &SubscribeStudentToCourse{
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
