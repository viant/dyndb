package fn

import "github.com/viant/dyndb/internal/exec"

func init() {
	exec.Register("array_exists", NewArrayExists)
}
