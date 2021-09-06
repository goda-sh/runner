package runner

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/gob"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"pkg.goda.sh/tasks"
	"pkg.goda.sh/utils"
)

var (
	// ISO8601 is the RegExp representation of an ISO8601 duration string (ex. P1Y2M3DT5H6M7S)
	ISO8601 = regexp.MustCompile(`(?i)P(?P<years>[\d\.]+Y)?(?P<months>[\d\.]+M)?(?P<days>[\d\.]+D)?T?(?P<hours>[\d\.]+H)?(?P<minutes>[\d\.]+M)?(?P<seconds>[\d\.]+?S)?`)
	// DurationMapping is the duration calculations for ISO8601
	DurationMapping = map[string]time.Duration{
		"years":   time.Hour * 24 * 365,
		"months":  time.Hour * 24 * 30,
		"days":    time.Hour * 24,
		"hours":   time.Hour,
		"minutes": time.Second * 60,
		"seconds": time.Second,
	}
)

// Identity describes the server
type Identity struct {
	MachineID string `json:"id"`
	Location  string `json:"location"`
}

// Runner describes the job runner instance
type Runner struct {
	RedisControl  tasks.Redis
	Identity      Identity
	TaskList      *utils.OrderedItems
	Paused        bool
	cancellations []context.CancelFunc
	OnResult      func(tasks.Task, tasks.Result)
	mu            sync.Mutex
}

// NewRunner creates a job runner instance
func NewRunner(id Identity, list []tasks.Task, rc tasks.Redis, OnResult func(tasks.Task, tasks.Result), paused bool) *Runner {
	return (&Runner{
		RedisControl:  rc,
		Identity:      id,
		TaskList:      utils.NewOrderedMap(),
		Paused:        paused,
		cancellations: make([]context.CancelFunc, 0),
		OnResult:      OnResult,
		mu:            sync.Mutex{},
	}).AddTasks(list)
}

// AddTasks adds a slice of tasks to the Runner
func (r *Runner) AddTasks(list []tasks.Task) *Runner {
	for _, t := range list {
		t.Location = r.Identity.Location
		r.Add(t)
	}
	return r
}

// Add adds a job to the queue
func (r *Runner) Add(t tasks.Task) *Runner {
	t.ID = r.Hash(t) // Hash the task for SSE + remote tasks
	ctx, cancel := context.WithCancel(context.Background())
	t.CTX = ctx
	t.Cancel = func() bool {
		cancel()
		select {
		case <-t.CTX.Done():
			return true
		default:
			log.Printf("Could not cancel %v (%s/%s)\n", t.Label, t.Task, t.ID)
			return false
		}
	}
	r.cancellations = append(r.cancellations, cancel)
	if typ, ok := tasks.TaskRunners[strings.ToLower(t.Task)]; ok {
		if tasks.Timerless(t.Task) {
			result := typ.Func(&tasks.TaskArgs{
				Task: r.TaskList.Add(t.ID, t).(tasks.Task),
				Callback: func(result tasks.Result) {
					t.Last = result.Update
					t.Warn = result.Warn
					t.Spark = result.Spark
					t.Date = time.Now().UnixNano() / int64(time.Millisecond)
					result.Location = r.Identity.Location
					r.OnResult(r.TaskList.Update(t.ID, t).(tasks.Task), result)
				},
				Redis: r.RedisControl,
			})
			if result.Error != nil {
				log.Printf("%s returned an error: %q - deleted: %v", t.Task, result.Error, r.TaskList.Del(t.ID))
			}
		} else {
			go func(t tasks.Task, duration time.Duration) bool {
				interval := time.Duration(250 * time.Millisecond)
				if t.Interval != "" {
					interval = r.ParseDuration(t.Interval)
				}
				ticker := time.NewTicker(duration)
				for {
					select {
					case <-ticker.C:
						if r.Paused {
							ticker.Reset(duration + (5 * time.Second))
							continue
						}
						ticker.Reset(interval)
						if result := typ.Func(&tasks.TaskArgs{
							Task:  t,
							Stop:  func() { ticker.Stop() },
							Redis: r.RedisControl,
						}); !result.Cancelled {
							t.Last = result.Update
							t.Warn = result.Warn
							t.Spark = result.Spark
							t.Date = time.Now().UnixNano() / int64(time.Millisecond)
							result.Location = r.Identity.Location
							r.OnResult(r.TaskList.Update(t.ID, t).(tasks.Task), result)
						}
					case <-t.CTX.Done():
						log.Printf("Removing %q (%s/%s) from task list.\n", t.Label, t.ID, t.Task)
						ticker.Stop()
						return r.TaskList.Del(t.ID)
					}
				}
			}(r.TaskList.Add(t.ID, t).(tasks.Task), time.Duration(r.TaskList.Count())*time.Second)
		}
	} else {
		log.Printf("skipping invalid task: %s", t.Task)
	}
	return r
}

// ParseDuration converts ISO8601 to time.Duration
func (r *Runner) ParseDuration(str string) (duration time.Duration) {
	match := ISO8601.FindStringSubmatch(str)
	for i, name := range ISO8601.SubexpNames() {
		if l := len(match[i]); l > 0 {
			if parsed, err := strconv.ParseFloat(match[i][:l-1], 64); err == nil {
				duration += time.Duration(float64(DurationMapping[name]) * parsed)
			}
		}
	}
	return
}

// Tasks gets a list of current tasks and their last result output
func (r *Runner) Tasks(name string) (out []tasks.CleanTask) {
	for task := range r.TaskList.Iter() {
		t := task.Value.(tasks.Task)
		if t.Last == nil {
			t.Last = struct{}{}
		}
		if len(name) == 0 || strings.EqualFold(t.Task, name) {
			out = append(out, tasks.CleanTask(t))
		}
	}
	return out
}

// Pause temporarily pauses task execution
func (r *Runner) Pause() {
	r.mu.Lock()
	r.Paused = true
	r.mu.Unlock()
}

// Resume restarts task execution
func (r *Runner) Resume() {
	r.mu.Lock()
	r.Paused = false
	r.mu.Unlock()
}

// Stop cancels all running tasks
func (r *Runner) Stop() {
	for _, cancel := range r.cancellations {
		cancel()
	}
}

// Hash generates a unique ID based on a task struct
func (r *Runner) Hash(t tasks.Task) string {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(tasks.Hash{
		Label:    t.Label,
		Interval: t.Interval,
		Task:     t.Task,
		ID:       t.ID,
		Once:     t.Once,
		Machine:  r.Identity.MachineID, // Use the MachineID to create a unique hash per node
	}); err == nil {
		return fmt.Sprintf("%x", md5.Sum(b.Bytes()))
	} else {
		log.Println(err)
	}
	return fmt.Sprintf("%s-%s", r.Identity.MachineID, uuid.Must(uuid.NewRandom()).String()) // Return MachineID + UUIDv4 if gob encoder fails
}
