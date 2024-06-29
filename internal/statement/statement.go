package statement

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

func (m *Manager) Get(name string) string {
	return m.statements[name]
}
