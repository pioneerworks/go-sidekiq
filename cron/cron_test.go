package cron

import (
	"context"
	"fmt"
	"testing"
	"time"

	sidekiq "github.com/pioneerworks/go-sidekiq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const TestEntryName = "test cron"

func NewTestCron(t *testing.T) *Cron {
	mgr, err := sidekiq.NewManager(sidekiq.Options{ServerAddr: "localhost:6379", Database: 0, PoolSize: 30, ProcessID: "1"})
	require.NoError(t, err)
	cron := NewCron(mgr)

	reset(mgr)

	return cron
}

func reset(mgr *sidekiq.Manager) {
	mgr.GetRedisClient().Del(context.Background(),
		"cron_jobs", // drop the cron job list
		fmt.Sprintf("cron_job:%s", TestEntryName),             // drop the entry details
		fmt.Sprintf("cron_job:%s:enqueued", TestEntryName),    // drop the enqueue history for the entry
		fmt.Sprintf("cron_job:%s:jid_history", TestEntryName), // drop the jid history
	)
}

func TestAddCrons(t *testing.T) {
	cron := NewTestCron(t)

	entry := &cronEntry{
		Name:        TestEntryName,
		Status:      "enabled",
		Description: "test cron tab for go sidekiq",
		Cron:        "*/5 * * * *",
		Job:         Job{Retry: "true", Queue: "default", Class: "GoWorker"},
	}

	cron.AddCron(entry)
}

func TestEnqueueOnce(t *testing.T) {
	asrt := assert.New(t)
	cron := NewTestCron(t)
	client := cron.mgr.GetRedisClient()

	entry, _ := NewCronEntry(TestEntryName, "", "*/5 * * * *", Enabled, "", "", nil, Job{Retry: "true", Queue: "default", Class: "GoWorker"})

	type testcase struct {
		name     string
		forTime  time.Time
		expected int64
	}

	cases := []testcase{
		{name: "enqueue the first job attempt", forTime: time.Date(2020, 01, 02, 11, 30, 12, 0, time.UTC), expected: 1},
		{name: "enqueue the second time for the same period will not enqueue", forTime: time.Date(2020, 01, 02, 11, 30, 12, 0, time.UTC), expected: 1},
	}

	for _, c := range cases {
		err := cron.EnqueueOnce(entry, c.forTime)

		asrt.NoError(err)
		r := client.ZCard(context.Background(), entry.EnqueuedKey())
		asrt.Equal(c.expected, r.Val())

	}
}
