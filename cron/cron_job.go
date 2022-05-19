package cron

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

type CronEntry struct {
	Name               string
	Klass              string
	Cron               string
	Description        string
	Args               []interface{}
	Status             string
	ActiveJob          string
	QueueNamePrefix    string
	QueueNameDelimiter string
	LastEnqueueTime    *time.Time
	Job                Job

	sched *cron.Schedule
}

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

const LastEnqueueTimeFormat = "2006-01-02 15:04:05 -0700"

func NewEntryFromMap(h map[string]string) (*CronEntry, error) {
	var args []interface{}
	err := json.Unmarshal([]byte(h["args"]), args)
	if err != nil {
		return nil, err
	}

	var job Job
	json.Unmarshal([]byte(h["message"]), &job)

	entry := &CronEntry{
		ActiveJob:          h["active_job"],
		Args:               args,
		Cron:               h["cron"],
		Description:        h["description"],
		Klass:              h["klass"],
		Name:               h["name"],
		QueueNameDelimiter: h["queue_name_delimiter"],
		QueueNamePrefix:    h["queue_name_prefix"],
		Status:             h["status"],
		Job:                job,
	}

	if le, ok := h["last_enqueue_time"]; ok && le != "" {
		lastEnqueue, err := time.Parse(LastEnqueueTimeFormat, le)
		if err != nil {
			return nil, err
		}
		entry.LastEnqueueTime = &lastEnqueue
	}
	return entry, nil
}

func (ce *CronEntry) EnqueuedKey() string {
	return fmt.Sprintf("cron_job:%s:enqueued", ce.Name)
}

func (ce *CronEntry) CronJobKey() string {
	return fmt.Sprintf("cron_job:%s", ce.Name)
}

func (ce *CronEntry) JIDHistoryKey() string {
	return fmt.Sprintf("cron_job:%s:jid_history", ce.Name)
}

// ShouldEnqueue returns true if the cron job is enabled, and the job is scheduled to run for the current minute.
// It's still possible to have a race condition, so the enqueuing logic should double check to make sure only one job is queued.
func (cj *CronEntry) ShouldEnqueue(now time.Time) bool {
	// is the job in a state where we can run?
	if cj.Status != "enabled" {
		return false
	}

	// is the job scheduled to run during the passed in time (minute resolution)?
	now = now.Truncate(time.Minute)
	next := cj.Next(now)
	if now != next {
		return false
	}

	// we can enqueue if there is no prior enqueue, or the last enqueued time is before teh passed in time.
	return cj.LastEnqueueTime == nil || cj.LastEnqueueTime.Before(now)
}

// Next returns the next scheduled runtime greater or equal to the current minute.
func (ce *CronEntry) Next(now time.Time) time.Time {
	if ce.sched == nil {
		sched, err := cronParser.Parse(ce.Cron)
		if err != nil {
			return time.Time{}
		}
		ce.sched = &sched
	}

	//To find out if the job runs during the  the current time, we step back one minute and then ask the cron schedule when the next run occurs.
	return (*ce.sched).Next(now.Add(time.Minute * -1))
}

type Job struct {
	Retry string        `json:"retry"`
	Queue string        `json:"queue"`
	Class string        `json:"class"`
	Args  []interface{} `json:"args"`
}

func (j Job) SafeArgs() []interface{} {
	if j.Args == nil {
		return []interface{}{}
	}
	return j.Args
}

func (cj *CronEntry) ToMap() map[string]string {
	args, err := json.Marshal(cj.Args)
	if err != nil {
		panic(err)
	}

	// ruby requires args be an array, even if empty. Probably a better way of doing this
	job := cj.Job
	if job.Args == nil {
		job.Args = []interface{}{}
	}

	msg, err := json.Marshal(job)
	if err != nil {
		panic(err)
	}

	return map[string]string{
		"active_job":           "false",
		"args":                 string(args),
		"cron":                 cj.Cron,
		"description":          cj.Description,
		"klass":                cj.Job.Class,
		"last_enqueue_time":    "",
		"message":              string(msg),
		"name":                 cj.Name,
		"queue_name_delimiter": cj.QueueNameDelimiter,
		"queue_name_prefix":    cj.QueueNamePrefix,
		"status":               cj.Status,
	}
}
