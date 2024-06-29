package statement

import (
	"fmt"
	"strings"
	"sync/atomic"
)

type Statement struct {
	Raw       string
	Format    string
	ParamOIDs []uint32
}

type Manager struct {
	statements map[string]Statement
	oidCounter atomic.Uint32
}

func NewManager() *Manager {
	c := atomic.Uint32{}
	c.Store(1000)
	return &Manager{
		statements: make(map[string]Statement),
		oidCounter: c,
	}
}

func (m *Manager) Add(name, sql string) {
	paramsN, format := replaceStmt(sql)
	paramsOIDs := make([]uint32, paramsN)
	for i := 0; i < paramsN; i++ {
		paramsOIDs[i] = m.oidCounter.Add(1)
	}
	m.statements[name] = Statement{
		Raw:       sql,
		Format:    format,
		ParamOIDs: paramsOIDs,
	}
}

func (m *Manager) Get(name string) (Statement, error) {
	stmt, ok := m.statements[name]
	if !ok {
		return Statement{}, fmt.Errorf("unknown statement: %s", name)
	}
	return stmt, nil
}

func (m *Manager) Bind(name string, args ...[]byte) (string, error) {
	stmt, ok := m.statements[name]
	if !ok {
		return "", fmt.Errorf("unknown statement: %s", name)
	}
	anyArgs := make([]any, 0, len(args))
	for _, arg := range args {
		anyArgs = append(anyArgs, arg)
	}
	return fmt.Sprintf(stmt.Format, anyArgs...), nil

}

func replaceStmt(s string) (int, string) {
	i := 1
	for {
		if !strings.Contains(s, fmt.Sprintf("$%d", i)) {
			break
		}
		s = strings.ReplaceAll(s, fmt.Sprintf("$%d", i), fmt.Sprintf("%[%d]", i))
		i++
	}
	return i - 1, s
}
