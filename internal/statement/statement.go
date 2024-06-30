package statement

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/record/schema"
)

type Prepared interface {
	Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType
	SwapParams(params map[int]schema.Constant) (Bound, error)
}

type Bound interface {
	Bound()
}

type Manager struct {
	statements map[string]Prepared
}

func NewManager() *Manager {
	return &Manager{
		statements: make(map[string]Prepared),
	}
}

func (m *Manager) Add(name string, prepared Prepared) {
	m.statements[name] = prepared
}

func (m *Manager) Get(name string) (Prepared, error) {
	stmt, ok := m.statements[name]
	if !ok {
		return nil, fmt.Errorf("unknown statement: %s", name)
	}
	return stmt, nil
}
