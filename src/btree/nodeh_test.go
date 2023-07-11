package btree

import "testing"

func TestNodeHeaderSize(t *testing.T) {
	size := nodeHeaderSize()
	var expected uint32 = 14
	if size != 14 {
		t.Fatalf("Wrong node header size: %d, expected %d", size, expected)
	}
}

func TestNodeBodySize(t *testing.T) {
	size := nodeBodySize()
	var expected uint32 = 4082
	if size != expected {
		t.Fatalf("Wrong node body size: %d, expected %d", size, expected)
	}
}
