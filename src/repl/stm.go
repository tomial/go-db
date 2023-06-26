package repl

import "strings"

type StatementType int
type PrepareStatementStatus int

const (
	PrepareStatementSuccess = iota
	PrepareStatementFailed
)

const (
	StatementTypeInsert StatementType = iota
	StatementTypeSelect
	StatementTypeInvalid
)

type statement struct {
	typ  StatementType
	op   string
	args []string
}

func prepareStm(ib *inputBuffer, stm *statement) PrepareStatementStatus {
	stm.op = ib.args[0]
	stm.args = ib.args[1:]
	switch strings.ToLower(stm.op) {
	case "insert":
		{
			stm.typ = StatementTypeInsert
		}
	case "select":
		{
			stm.typ = StatementTypeSelect
		}
	default:
		{
			stm.typ = StatementTypeInvalid
		}
	}

	if stm.typ == StatementTypeInvalid {
		return PrepareStatementFailed
	} else {
		return PrepareStatementSuccess
	}
}

func (stm *statement) Execute() {

}
