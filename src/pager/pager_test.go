package pager

import (
	"db/src/constants"
	"io"
	"os"
	"testing"
)

func TestWritePage(t *testing.T) {
	file, _ := os.OpenFile(constants.DbFileName, os.O_RDWR|os.O_CREATE, 0755)
	pager := Init(file)

	buf := make([]byte, constants.PageSize)
	buf[0] = 0xAB
	buf[1] = 0xCD
	pager.WritePage(0, buf)

	verifyBuf := make([]byte, 2)
	pager.File.ReadAt(verifyBuf, io.SeekStart)
	if verifyBuf[0] != 0xAB || verifyBuf[1] != 0xCD {
		t.Fatalf("Pager: failed to write certain page")
	}

	buf = make([]byte, constants.PageSize)
	buf[0] = 0xEF
	buf[1] = 0xFE
	pager.WritePage(1, buf)
	verifyBuf = make([]byte, 2)
	pager.File.ReadAt(verifyBuf, int64(constants.PageSize))
	if verifyBuf[0] != 0xEF || verifyBuf[1] != 0xFE {
		t.Fatalf("Pager: failed to write certain page")
	}
}

func TestReadPage(t *testing.T) {
	file, _ := os.OpenFile(constants.DbFileName, os.O_RDWR|os.O_CREATE, 0755)
	pager := Init(file)
	data := pager.ReadPage(0)
	if data[0] != 0xAB || data[1] != 0xCD {
		t.Fatalf("Pager: failed to read certain page")
	}

	data = pager.ReadPage(1)
	if data[0] != 0xEF || data[1] != 0xFE {
		t.Fatalf("Pager: failed to read certain page")
	}
}
