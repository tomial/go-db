package repl

import (
	"log"
	"os"
)

type MetaCommandType int
type MetaCommandResult int

const (
	MetaCmdTypeExit MetaCommandType = iota
	MetaCmdTypeUnrecognized
)

const (
	MetaCmdResultSuccess MetaCommandResult = iota
	MetaCmdResultFailed
	MetaCmdResultPending
	MetaCmdResultUnrecognized
)

type metaCommand struct {
	str      string
	typ      MetaCommandType
	result   MetaCommandResult
	callback func()
}

func executeMetaCmd(ib *inputBuffer) {
	op := ib.args[0]

	var metacmd metaCommand
	metacmd.str = op

	switch op {
	case ".exit":
		{
			metacmd.typ = MetaCmdTypeExit
			metacmd.result = MetaCmdResultPending
			metacmd.callback = func() {
				os.Exit(0)
			}
		}
	default:
		{
			metacmd.typ = MetaCmdTypeUnrecognized
			metacmd.result = MetaCmdResultUnrecognized
		}
	}

	if metacmd.result == MetaCmdResultPending {
		metacmd.callback()
	} else {
		metacmd.result = MetaCmdResultUnrecognized
	}

	switch metacmd.result {
	case MetaCmdResultFailed:
		log.Printf("Failed to execute meta command: %v\n", metacmd.str)
	case MetaCmdResultUnrecognized:
		log.Printf("Failed to recognize meta command: %v\n", metacmd.str)
	default:
		log.Printf("Unhandled meta command result: %v", metacmd.result)
	}
}
