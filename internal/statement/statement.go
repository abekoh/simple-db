package statement

import (
	"fmt"
	"strings"
)

type Manager struct {
	statements map[string]string
}

func NewManager() *Manager {
	return &Manager{
		statements: make(map[string]string),
	}
}

func (m *Manager) Add(name, sql string) {
	m.statements[name] = sql
}

func (m *Manager) Bind(name string, args ...any) (string, error) {
	stmt, ok := m.statements[name]
	if !ok {
		return "", fmt.Errorf("unknown statement: %s", name)
	}
	sql := replaceStmt(stmt)
	return fmt.Sprintf(sql, args...), nil

}

func replaceStmt(s string) string {
	i := 1
	for {
		if !strings.Contains(s, fmt.Sprintf("$%d", i)) {
			break
		}
		s = strings.ReplaceAll(s, fmt.Sprintf("$%d", i), fmt.Sprintf("%[%d]", i))
		i++
	}
	return s
}
