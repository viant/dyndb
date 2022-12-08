package dyndb

type tx struct {
	*Connection
}

func (t *tx) Commit() error {
	return nil
}

func (t *tx) Rollback() error {
	return nil
}
