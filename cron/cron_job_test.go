package cron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldEnqueueJob(t *testing.T) {
	cron := "0 * * * *"
	type testCase struct {
		name     string
		forTime  time.Time
		entry    cronEntry
		expected bool
	}

	enabledEntry, _ := NewCronEntry("test_job", "", cron, Enabled, "", "", nil, Job{})
	disabledEntry, _ := NewCronEntry("test_job", "", cron, Disabled, "", "", nil, Job{})

	oldEnqueuedEntry, _ := NewCronEntry("test_job", "", cron, Enabled, "", "", nil, Job{})
	oldEnqueuedEntry.LastEnqueueTime = newTime(10, 59, 55)

	currentEnqueuedEntry, _ := NewCronEntry("test_job", "", cron, Enabled, "", "", nil, Job{})
	currentEnqueuedEntry.LastEnqueueTime = newTime(11, 0, 0)

	newerEnqueuedEntry, _ := NewCronEntry("test_job", "", cron, Enabled, "", "", nil, Job{})
	newerEnqueuedEntry.LastEnqueueTime = newTime(11, 1, 0)

	testCases := []testCase{
		// time based scenarios
		{
			name:     "returns true at the scheduled time",
			forTime:  atTime(11, 0, 0),
			entry:    *enabledEntry,
			expected: true,
		},
		{
			name:     "returns true if the current time is  during the scheduled minute",
			forTime:  atTime(11, 0, 55),
			entry:    *enabledEntry,
			expected: true,
		},
		{
			name:     "returns false if the time is one min before the schedule minute",
			forTime:  atTime(10, 59, 0),
			entry:    *enabledEntry,
			expected: false,
		},
		{
			name:     "returns false if the time is one min after the schedule minute",
			forTime:  atTime(11, 1, 0),
			expected: false,
			entry:    *enabledEntry,
		},
		// status scenarios
		{
			name:     "returns true if the job is enabled ",
			forTime:  atTime(11, 0, 0),
			expected: true,
			entry:    *enabledEntry,
		},
		{
			name:     "returns false if the job is disabled ",
			forTime:  atTime(11, 0, 0),
			expected: false,
			entry:    *disabledEntry,
		},
		// last enqueue time scenarios
		{
			name:     "returns true if the job has never been enqueued",
			forTime:  atTime(11, 0, 0),
			expected: true,
			entry:    *enabledEntry,
		},
		{
			name:     "returns true if the last enqueue is old",
			forTime:  atTime(11, 0, 0),
			expected: true,
			entry:    *oldEnqueuedEntry,
		},
		{
			name:     "returns false if the last enqueue matches the current scheduled run",
			forTime:  atTime(11, 0, 0),
			expected: false,
			entry:    *currentEnqueuedEntry,
		},
		{
			name:     "returns false if the last enqueue is newer than the current scheduled run",
			forTime:  atTime(11, 0, 0),
			expected: false,
			entry:    *newerEnqueuedEntry,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expected, c.entry.ShouldEnqueue(c.forTime))
		})
	}
}

func TestToFromMap(t *testing.T) {
	asrt := assert.New(t)
	rqr := require.New(t)

	entry, err := NewCronEntry("test_job", "", "* * * * *", EntryStatus("enabled"), "", "", nil, Job{Retry: "true", Class: "TestClass", Queue: "TestQueue"})
	rqr.NoError(err)

	m := entry.ToMap()
	rqr.NotNil(m)
	asrt.Equal("enabled", m["status"])
	asrt.Equal("test_job", m["name"])
	asrt.Equal("* * * * *", m["cron"])
	asrt.Equal("[]", m["args"])
	asrt.Equal("", m["last_enqueued_time"])

	_, err = CronEntryFromMap(m)
	rqr.NoError(err)
	// t.Logf("%#v", entry2)
}

func newTime(hour, min, sec int) *time.Time {
	t := atTime(hour, min, sec)
	return &t
}

func atTime(hour, min, sec int) time.Time {
	curTime := time.Date(2020, 01, 02, hour, min, sec, 0, time.UTC)
	return curTime
}
