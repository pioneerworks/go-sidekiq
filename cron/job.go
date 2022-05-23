package cron

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
