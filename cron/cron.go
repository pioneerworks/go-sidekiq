package cron

import (
	"context"
	"encoding/json"
	"time"

	sidekiq "github.com/pioneerworks/go-sidekiq"

	"github.com/go-redis/redis/v8"
)

type Cron struct {
	mgr *sidekiq.Manager
	log Logger
}
type Logger interface {
	Printf(format string, v ...any)
	Println(v ...any)
}

// NewCron creates a new instance of the cron manager. You can use the Cron object to add and remove cron entries and to start polling.
// NewCron expects a valid sidekiq manager which it will use to interact with redis, and to enqueue jobs with.
// log can optionally be set, and will delegate logging to the passed in object if it is not nil.
func NewCron(m *sidekiq.Manager, log Logger) *Cron {
	return &Cron{mgr: m, log: &safeLogger{log}}
}

func (c *Cron) Poll(ctx context.Context, interval time.Duration) {

	ticker := time.NewTicker(interval)
	for {
		select {
		case now := <-ticker.C:
			c.log.Println("polling jobs")
			entries, err := c.CronEntries(ctx)
			if err != nil {
				panic(err)
			}
			err = c.enqueueEntries(ctx, entries, now)
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

// enqueueEntries takes into account the LastEnqueuedTime and only allows a job to be enqueued once per scheduled period.
// The atomic behavior of the ZAdd operation makes this operation safe across multiple processes.
func (c *Cron) enqueueEntries(ctx context.Context, entries []*cronEntry, forTime time.Time) error {
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if entry.ShouldEnqueue(forTime) {
				score := float64(forTime.Unix())
				member := entry.Next(forTime).Format(LastEnqueueTimeFormat)
				r := c.mgr.GetRedisClient().ZAdd(context.Background(), entry.EnqueuedKey(), &redis.Z{Score: score, Member: member})
				if r.Val() == 1 {
					c.Enqueue(ctx, entry, forTime)
					c.log.Printf("enqueued job for cron %s, at %s", entry.Name, member)
				}
				return r.Err()
			}
		}
	}
	return nil
}

// Enqueue adds a job to sidekiq and updates the cron entry with the enqueue time.
func (c *Cron) Enqueue(ctx context.Context, entry *cronEntry, forTime time.Time) bool {
	// enqueue
	job := entry.Job
	args := job.SafeArgs()
	jid, err := c.mgr.Producer().Enqueue(job.Queue, job.Class, args)
	if err != nil {
		return false
	}

	client := c.mgr.GetRedisClient()

	enqueuedAt := forTime.Format(LastEnqueueTimeFormat)
	c.mgr.GetRedisClient().HSet(ctx, entry.CronJobKey(), "last_enqueued_at", enqueuedAt)
	hist, _ := json.Marshal(map[string]string{"jid": jid, "enqueued": enqueuedAt})
	client.LPush(ctx, entry.JIDHistoryKey(), hist)

	return true
}

// CronEntries returns all of the entries associated with the sidekiq instance.
func (c *Cron) CronEntries(ctx context.Context) ([]*cronEntry, error) {
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

// AddCron adds a cron entry to sidekiq. If the cron entry already exists, its state will be updated.
func (c *Cron) AddCron(ctx context.Context, e *cronEntry) error {
	cmd := c.mgr.GetRedisClient().SAdd(ctx, "cron_jobs", e.CronJobKey())
	if cmd.Err() != nil {
		return cmd.Err()
	}

	c.mgr.GetRedisClient().HSet(ctx, e.CronJobKey(), e.ToMap())
	return nil
}

// safeLogger ensures the underlying logger is valid
type safeLogger struct {
	log Logger
}

func (sf *safeLogger) Printf(format string, v ...any) {
	if sf != nil && sf.log != nil {
		sf.log.Printf(format, v...)
	}
}

func (sf *safeLogger) Println(v ...any) {
	if sf != nil && sf.log != nil {
		sf.log.Println(v...)
	}
}
