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

	entry := &CronEntry{
		Name:        TestEntryName,
		Status:      "enabled",
		Description: "test cron tab for go sidekiq",
		Cron:        "*/5 * * * *",
		Job:         Job{Retry: "true", Queue: "default", Class: "GoWorker"},
	}

	cron.AddCron(entry)
}

func TestEnqueue(t *testing.T) {
	asrt := assert.New(t)
	cron := NewTestCron(t)

	entry := &CronEntry{
		Name:        TestEntryName,
		Status:      "enabled",
		Description: "test cron tab for go sidekiq",
		Cron:        "*/5 * * * *",
		Job:         Job{Retry: "true", Queue: "default", Class: "GoWorker"},
	}

	curTime := time.Date(2020, 01, 02, 11, 33, 12, 0, time.UTC)
	asrt.True(cron.Enqueue(entry, curTime))
	asrt.False(cron.Enqueue(entry, curTime), "requeuing the job at the same time should fail")
}
