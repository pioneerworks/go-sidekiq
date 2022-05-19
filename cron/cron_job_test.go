package cron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShouldEnqueueJob(t *testing.T) {
	cron := "0 * * * *"
	type testCase struct {
		name     string
		forTime  time.Time
		entry    CronEntry
		expected bool
	}

	testCases := []testCase{
		// time based scenarios
		{
			name:     "returns true at the scheduled time",
			forTime:  atTime(11, 0, 0),
			entry:    CronEntry{Status: "enabled", Name: "test_job", Cron: cron},
			expected: true,
		},
		{
			name:     "returns true if the current time is  during the scheduled minute",
			forTime:  atTime(11, 0, 55),
			entry:    CronEntry{Status: "enabled", Name: "test_job", Cron: cron},
			expected: true,
		},
		{
			name:     "returns false if the time is one min before the schedule minute",
			forTime:  atTime(10, 59, 0),
			entry:    CronEntry{Status: "enabled", Name: "test_job", Cron: cron},
			expected: false,
		},
		{
			name:     "returns false if the time is one min after the schedule minute",
			forTime:  atTime(11, 1, 0),
			expected: false,
			entry:    CronEntry{Status: "enabled", Name: "test_job", Cron: cron},
		},
		// status scenarios
		{
			name:     "returns true if the job is enabled ",
			forTime:  atTime(11, 0, 0),
			expected: true,
			entry:    CronEntry{Status: "enabled", Name: "test_job", Cron: cron},
		},
		{
			name:     "returns false if the job is disabled ",
			forTime:  atTime(11, 0, 0),
			expected: false,
			entry:    CronEntry{Status: "disabled", Name: "test_job", Cron: cron},
		},
		// last enqueue time scenarios
		{
			name:     "returns true if the job has never been enqueued",
			forTime:  atTime(11, 0, 0),
			expected: true,
			entry:    CronEntry{Status: "enabled", Name: "test_job", Cron: cron, LastEnqueueTime: nil},
		},
		{
			name:     "returns true if the last enqueue is old",
			forTime:  atTime(11, 0, 0),
			expected: true,
			entry:    CronEntry{Status: "enabled", Name: "test_job", Cron: cron, LastEnqueueTime: newTime(10, 59, 55)},
		},
		{
			name:     "returns false if the last enqueue matches the current scheduled run",
			forTime:  atTime(11, 0, 0),
			expected: false,
			entry:    CronEntry{Status: "enabled", Name: "test_job", Cron: cron, LastEnqueueTime: newTime(11, 0, 0)},
		},
		{
			name:     "returns false if the last enqueue is newer than the current scheduled run",
			forTime:  atTime(11, 0, 0),
			expected: false,
			entry:    CronEntry{Status: "enabled", Name: "test_job", Cron: cron, LastEnqueueTime: newTime(11, 1, 0)},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expected, c.entry.ShouldEnqueue(c.forTime))
		})
	}

}

func newTime(hour, min, sec int) *time.Time {
	t := atTime(hour, min, sec)
	return &t
}

func atTime(hour, min, sec int) time.Time {
	curTime := time.Date(2020, 01, 02, hour, min, sec, 0, time.UTC)
	return curTime
}
