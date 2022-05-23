package cron

import (
	"context"
	"encoding/json"
	"time"

	sidekiq "github.com/pioneerworks/go-sidekiq"

	"github.com/go-redis/redis/v8"
)

type Cron struct {
	mgr   *sidekiq.Manager
	clock *clock
}

func NewCron(m *sidekiq.Manager) *Cron {
	return &Cron{mgr: m}
}

func (c *Cron) Poll(interval time.Duration) {
	forTime := c.clock.Now()
	entries, err := c.CronEntries()
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		if entry.ShouldEnqueue(forTime) {
			c.EnqueueOnce(entry, forTime)
		}
	}
}

// EnqueueOnce takes into account the LastEnqueuedTime and only allows a job to be enqueued once per scheduled period.
// The atomic behavior of the ZAdd operation makes this operation safe across multiple processes.
func (c *Cron) EnqueueOnce(entry *cronEntry, forTime time.Time) error {
	score := float64(forTime.Unix())
	member := entry.Next(forTime).Format(LastEnqueueTimeFormat)
	r := c.mgr.GetRedisClient().ZAdd(context.Background(), entry.EnqueuedKey(), &redis.Z{Score: score, Member: member})
	if r.Val() == 1 {
		c.Enqueue(entry, forTime)
	}
	return r.Err()
}

func (c *Cron) Enqueue(entry *cronEntry, forTime time.Time) bool {
	// enqueue
	job := entry.Job
	args := job.SafeArgs()
	jid, err := c.mgr.Producer().Enqueue(job.Queue, job.Class, args)
	if err != nil {
		return false
	}

	client := c.mgr.GetRedisClient()
	ctx := context.Background()

	enqueuedAt := forTime.Format(LastEnqueueTimeFormat)
	c.mgr.GetRedisClient().HSet(context.Background(), entry.CronJobKey(), "last_enqueued_at", enqueuedAt)
	hist, _ := json.Marshal(map[string]string{"jid": jid, "enqueued": enqueuedAt})
	client.LPush(ctx, entry.JIDHistoryKey(), hist)

	return true
}

func (c *Cron) CronEntries() ([]*cronEntry, error) {
	ctx := context.Background()
	redis := c.mgr.GetRedisClient()
	cmd := redis.SMembers(ctx, "cron_jobs")
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	var entries []*cronEntry
	for _, val := range cmd.Val() {
		r := c.mgr.GetRedisClient().HGetAll(ctx, val)
		if r.Err() != nil {
			return nil, cmd.Err()
		}
		entry, _ := CronEntryFromMap(r.Val())
		entries = append(entries, entry)
	}
	return entries, nil
}

func (c *Cron) AddCron(e *cronEntry) error {
	ctx := context.Background()

	cmd := c.mgr.GetRedisClient().SAdd(ctx, "cron_jobs", e.CronJobKey())
	if cmd.Err() != nil {
		return cmd.Err()
	}

	c.mgr.GetRedisClient().HSet(ctx, e.CronJobKey(), e.ToMap())
	return nil
}
