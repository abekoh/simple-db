package simpledb

import "testing"

func TestFile(t *testing.T) {
	fm := NewFileManager()
	blk := NewBlockID("testfile", 2)
	p1 := NewPageBlocksize(fm.Blocksize)
	var pos1 int64 = 88
	if err := p1.SetStr(pos1, "abcdefghijklmn"); err != nil {
		t.Fatal(err)
	}
	size := MaxLength(len("abcdefghijklmn"))
	var pos2 int64 = pos1 + size
	p1.SetInt(pos2, 345)
	if err := fm.Write(blk, p1); err != nil {
		t.Fatal(err)
	}

	p2 := NewPageBlocksize(fm.Blocksize)
	if err := fm.Read(blk, p2); err != nil {
		t.Fatal(err)
	}

	got1 := p2.Int(pos2)
	if got1 != 345 {
		t.Error()
	}

	got2 := p2.Str(pos1)
	if got2 != "abcdefghijklmn" {
		t.Error()
	}
}
