package dyndb

import (
	"github.com/viant/dyndb/internal/exec"
	"sync"
)

type executions struct {
	maxSize int
	sync.RWMutex
	cache      map[string]int
	executions []*exec.Execution
}

func (e *executions) Put(execution *exec.Execution) {
	if e.maxSize < 1 {
		return
	}
	e.RWMutex.Lock()
	defer e.RWMutex.Unlock()
	if len(e.executions)+1 > e.maxSize {
		e.cache = map[string]int{}
		e.executions = e.executions[:0]
	}
	size := len(e.executions)
	e.cache[execution.SQL] = size
	e.executions = append(e.executions, execution)
}

func (e *executions) Lookup(SQL string) *exec.Execution {
	e.RWMutex.RLock()
	pos, ok := e.cache[SQL]
	defer e.RWMutex.RUnlock()
	if !ok {
		return nil
	}
	return e.executions[pos]
}
