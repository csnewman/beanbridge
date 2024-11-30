package backend

type Backend interface {
	ResolveTube(name string) Tube

	Put(tube Tube, pri uint64, delay uint64, ttr uint64, data []byte) (uint64, bool, error)

	Reserve(tubes []Tube, timeout int64) (uint64, []byte, error)
}

type Tube interface {
	Name() string

	Release()
}
