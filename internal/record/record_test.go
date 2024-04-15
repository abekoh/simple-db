package record

import (
	"testing"
)

func TestLayout(t *testing.T) {
	t.Parallel()

	s := NewSchema()
	s.AddInt32Field("A")
	s.AddStrField("B", 9)
	l := NewLayoutSchema(s)
	if l.Offset("A") != 4 {
		t.Errorf("expected 0, got %d", l.Offset("A"))
	}
	if l.Offset("B") != 8 {
		t.Errorf("expected 8, got %d", l.Offset("B"))
	}
}
