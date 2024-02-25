package file

import "testing"

func TestPage(t *testing.T) {
	t.Parallel()
	t.Run("Int32 with offset 0", func(t *testing.T) {
		t.Parallel()
		p := NewPage(128)
		p.SetInt32(0, 123)
		if p.Int32(0) != 123 {
			t.Errorf("expected 123, got %d", p.Int32(0))
		}
	})
	t.Run("Int32 with offset 4", func(t *testing.T) {
		t.Parallel()
		p := NewPage(128)
		p.SetInt32(4, 123)
		if p.Int32(4) != 123 {
			t.Errorf("expected 123, got %d", p.Int32(4))
		}
	})
	t.Run("Bytes with offset 0", func(t *testing.T) {
		t.Parallel()
		p := NewPage(128)
		p.SetBytes(0, []byte{1, 2, 3, 4})
		if string(p.Bytes(0)) != "\x01\x02\x03\x04" {
			t.Errorf("expected \\x01\\x02\\x03\\x04, got %q", p.Bytes(0))
		}
	})
	t.Run("Bytes with offset 4", func(t *testing.T) {
		t.Parallel()
		p := NewPage(128)
		p.SetBytes(4, []byte{1, 2, 3, 4})
		if string(p.Bytes(4)) != "\x01\x02\x03\x04" {
			t.Errorf("expected \\x01\\x02\\x03\\x04, got %q", p.Bytes(4))
		}
	})
	t.Run("Str with offset 0", func(t *testing.T) {
		t.Parallel()
		p := NewPage(128)
		p.SetStr(0, "abcd")
		if p.Str(0) != "abcd" {
			t.Errorf("expected abcd, got %s", p.Str(0))
		}
	})
	t.Run("Str with offset 4", func(t *testing.T) {
		t.Parallel()
		p := NewPage(128)
		p.SetStr(4, "abcd")
		if p.Str(4) != "abcd" {
			t.Errorf("expected abcd, got %s", p.Str(4))
		}
	})
	t.Run("NewPageBytes", func(t *testing.T) {
		t.Parallel()
		p := NewPageBytes([]byte("abcd"))
		if string(p.bb) != "abcd" {
			t.Errorf("expected abcd, got %s", p.bb)
		}
	})
}

func TestFileManager(t *testing.T) {
	t.Parallel()
	t.Run("Read and write", func(t *testing.T) {
		t.Parallel()
		fm, err := NewFileManager(t.TempDir(), 128)
		if err != nil {
			t.Fatal(err)
		}

		blockID := NewBlockID("testfile", 0)
		writeP := NewPage(128)
		writeP.SetStr(0, "abcd")
		readP := NewPage(128)

		err = fm.Write(blockID, writeP)
		if err != nil {
			t.Fatal(err)
		}
		err = fm.Read(blockID, readP)
		if err != nil {
			t.Fatal(err)
		}

		if string(readP.Str(0)) != "abcd" {
			t.Errorf("expected abcd, got %s", readP.Str(0))
		}
	})
}
