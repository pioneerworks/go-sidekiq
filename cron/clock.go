package cron

import "time"

type clock struct {
	t time.Time
}

func (c *clock) Now() time.Time {
	if c == nil {
		return time.Now()
	}
	return c.t
}
