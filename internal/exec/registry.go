package exec

import "sync"

//Register register custom function
func Register(name string, newFunc NewFunc) {
	funcRegistry.Register(name, newFunc)
}

//Lookup returns custom function
func Lookup(name string) NewFunc {
	return funcRegistry.Lookup(name)
}

type registry struct {
	sync.RWMutex
	items map[string]NewFunc
}

var funcRegistry = &registry{items: map[string]NewFunc{}}

func (r *registry) Register(name string, newFunc NewFunc) {
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	r.items[name] = newFunc
}

func (r *registry) Lookup(name string) NewFunc {
	r.RWMutex.RLock()
	defer r.RWMutex.RUnlock()
	return r.items[name]
}
