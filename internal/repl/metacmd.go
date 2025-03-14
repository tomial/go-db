package repl

import (
	"log"
	"os"
)

type MetaCommandType int
type MetaCommandResult int

const (
	MetaCmdTypeExit MetaCommandType = iota
	MetaCmdHelp
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

func (m *metaCommand) printHelp() {
	var prompt = `
	commands:
	- insert [id] [username] [email]: insert new row [id username email]
	- select [id]: select the row with [id]
	- .help: print help
	- .exit: quit
	`
	log.Println(prompt)
}

func (m *metaCommand) exit() {
	os.Exit(0)
}

func executeMetaCmd(ib *inputBuffer) {
	op := ib.args[0]

	metacmd := &metaCommand{}
	metacmd.str = op

	switch op {
	case ".exit":
		{
			metacmd.typ = MetaCmdTypeExit
			metacmd.result = MetaCmdResultPending
			metacmd.callback = metacmd.exit
		}
	case ".help":
		{
			metacmd.typ = MetaCmdHelp
			metacmd.result = MetaCmdResultPending
			metacmd.callback = metacmd.printHelp
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
	}
}
