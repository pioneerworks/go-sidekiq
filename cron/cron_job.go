package cron

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

type cronEntry struct {
	Name               string
	Cron               string
	Description        string
	Args               []interface{}
	Status             string
	ActiveJob          string
	QueueNamePrefix    string
	QueueNameDelimiter string
	LastEnqueueTime    *time.Time
	Job                Job

	klass string
	sched cron.Schedule
}

type EntryStatus string

const (
	Enabled  EntryStatus = "enabled"
	Disabled EntryStatus = "disabled"
)

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

const LastEnqueueTimeFormat = "2006-01-02 15:04:05 -0700"

func NewCronEntry(name, desc, cron string, status EntryStatus, queuePrefix, queueDelimiter string, args []interface{}, job Job) (*cronEntry, error) {
	sched, err := cronParser.Parse(cron)
	if err != nil {
		return nil, fmt.Errorf("the cron line is not valid: %w", err)
	}

	if args == nil {
		args = []interface{}{} // the ruby implementation expects a non-nil array
	}

	return &cronEntry{
		Name:               name,
		Cron:               cron,
		Description:        desc,
		Args:               args,
		Status:             string(status),
		QueueNamePrefix:    queuePrefix,
		QueueNameDelimiter: queueDelimiter,
		Job:                job,

		sched: sched,
		klass: job.Class,
	}, nil
}

func (ce *cronEntry) EnqueuedKey() string {
	return fmt.Sprintf("cron_job:%s:enqueued", ce.Name)
}

func (ce *cronEntry) CronJobKey() string {
	return fmt.Sprintf("cron_job:%s", ce.Name)
}

func (ce *cronEntry) JIDHistoryKey() string {
	return fmt.Sprintf("cron_job:%s:jid_history", ce.Name)
}

// ShouldEnqueue returns true if the cron job is enabled, and the job is scheduled to run for the current minute.
// It's still possible to have a race condition, so the enqueuing logic should double check to make sure only one job is queued.
func (ce *cronEntry) ShouldEnqueue(now time.Time) bool {
	// is the job in a state where we can run?
	if ce.Status != "enabled" {
		return false
	}

	// is the job scheduled to run during the passed in time (minute resolution)?
	now = now.Truncate(time.Minute)
	next := ce.Next(now)
	if now != next {
		return false
	}

	// we can enqueue if there is no prior enqueue, or the last enqueued time is before teh passed in time.
	return ce.LastEnqueueTime == nil || ce.LastEnqueueTime.Before(now)
}

// Next returns the next scheduled runtime greater or equal to the current minute.
func (ce *cronEntry) Next(now time.Time) time.Time {
	//To find out if the job runs during the  the current time, we step back one minute and then ask the cron schedule when the next run occurs.
	return ce.sched.Next(now.Add(time.Minute * -1))
}

var cronEntryFields = []string{"name", "description", "cron", "status", "active_job", "queue_name_prefix", "queue_name_delimiter", "args", "message", "last_enqueue_time"}

// ToMap converts the CronEntry into a map that matches the ruby cron job expectations.
func (ce *cronEntry) ToMap() map[string]string {
	// jobs and args are stored as json in a redis hash
	args, _ := json.Marshal(ce.Args)
	msg, _ := json.Marshal(ce.Job)

	// The LastEnqueueTime needs to match the format used by sidekiq::cron
	var lastEnqTime string
	if ce.LastEnqueueTime != nil {
		lastEnqTime = ce.LastEnqueueTime.Format(LastEnqueueTimeFormat)
	}

	return map[string]string{
		"active_job":           ce.ActiveJob,
		"args":                 string(args),
		"cron":                 ce.Cron,
		"description":          ce.Description,
		"klass":                ce.Job.Class,
		"last_enqueue_time":    lastEnqTime,
		"message":              string(msg),
		"name":                 ce.Name,
		"queue_name_delimiter": ce.QueueNameDelimiter,
		"queue_name_prefix":    ce.QueueNamePrefix,
		"status":               ce.Status,
	}
}
func CronEntryFromMap(m map[string]string) (*cronEntry, error) {
	var (
		args []interface{}
		job  Job
		err  error
	)

	// make sure all required fields are present
	for _, field := range cronEntryFields {
		if _, ok := m[field]; !ok {
			return nil, fmt.Errorf("field %s is missing from the cron entry", field)
		}
	}

	if err = json.Unmarshal([]byte(m["args"]), &args); err != nil {
		return nil, err
	}

	if err = json.Unmarshal([]byte(m["message"]), &job); err != nil {
		return nil, err
	}

	entry, err := NewCronEntry(m["name"], m["description"], m["cron"], EntryStatus(m["status"]), m["queue_name_prefix"], m["queue_name_delimiter"], args, job)
	if err != nil {
		return nil, err
	}

	var lastEnqueue *time.Time
	if le, ok := m["last_enqueue_time"]; ok && le != "" {
		enq, err := time.Parse(LastEnqueueTimeFormat, le)
		if err != nil {
			return nil, err
		}
		lastEnqueue = &enq
	}

	entry.ActiveJob = m["active_job"]
	entry.LastEnqueueTime = lastEnqueue
	return entry, nil
}
