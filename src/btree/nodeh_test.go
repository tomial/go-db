package btree

import "testing"

func TestNodeHeaderSize(t *testing.T) {
	size := nodeHeaderSize()
	var expected uint32 = 16
	if size != expected {
		t.Fatalf("Wrong node header size: %d, expected %d", size, expected)
	}
}

func TestNodeBodySize(t *testing.T) {
	size := nodeBodySize()
	var expected uint32 = 4080
	if size != expected {
		t.Fatalf("Wrong node body size: %d, expected %d", size, expected)
	}
}
