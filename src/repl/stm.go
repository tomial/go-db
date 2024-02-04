package repl

import (
	"db/src/row"
	"log"
	"reflect"
	"strconv"
	"strings"
)

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
	row  row.Row
}

func prepareStm(ib *inputBuffer, stm *statement) PrepareStatementStatus {
	stm.op = ib.args[0]
	stm.args = ib.args
	switch strings.ToLower(stm.op) {
	case "insert":
		{
			// TODO Creating table struct with code automatically
			// TODO Support generic rows
			stm.typ = StatementTypeInsert

			row := row.UserRow{}
			val := reflect.ValueOf(&row).Elem()

			if len(stm.args) != val.NumField() {
				log.Printf("Prepare statement: Incorrect argument amount, expected %d, found %d\n", len(stm.args), val.NumField())
				return PrepareStatementFailed
			}
			// fill args into row fields
			for i := 0; i < val.NumField(); i++ {
				field := val.Field(i)
				arg := stm.args[i]

				if field.Kind() != reflect.Struct {
					if field.CanSet() {
						switch field.Kind() {
						case reflect.String:
							{
								field.SetString(arg)
							}
						case reflect.Uint64:
							{
								num, err := strconv.ParseUint(arg, 10, 64)
								if err != nil {
									log.Println("Prepare statement: Failed to convert arg to uint64")
									return PrepareStatementFailed
								}
								field.SetUint(num)
							}
						case reflect.Int64:
							{
								num, err := strconv.ParseInt(arg, 10, 64)
								if err != nil {
									log.Println("Prepare statement: Failed to convert arg to int64")
									return PrepareStatementFailed
								}
								field.SetInt(num)
							}
						default:
							{
								log.Printf("Prepare statement: Not supported type: %v", field.Kind())
								return PrepareStatementFailed
							}
						}
					}
				}
			}
			stm.row = &row
		}
	case "select":
		{
			stm.typ = StatementTypeSelect
			row := &row.UserRow{}
			row.TableName = "User"
			stm.row = row
		}
	default:
		{
			stm.typ = StatementTypeInvalid
		}
	}

	return PrepareStatementSuccess
}

func (stm *statement) Execute() {
	switch stm.typ {
	case StatementTypeSelect:
		{
			runSelect(stm)
		}
	case StatementTypeInsert:
		{
			runInsert(stm)
		}
	case StatementTypeInvalid:
		{
			log.Println("Execute statement error: Invalid statement type")
		}
	default:
		{
			log.Printf("Execute statement error: Unhandled statement type: %v\n", stm.typ)
		}
	}
}

func runSelect(stm *statement) {
	index, err := strconv.ParseUint(stm.args[1], 10, 64)
	if err != nil {
		log.Printf("Failed to run select, error parsing row index: %s\n", err)
	}
	stm.row.InitCursor(uint32(index))
	err = stm.row.Load()
	if err != nil {
		log.Printf("Failed to run select, error loading data: %s\n", err)
	}
}

func runInsert(stm *statement) {
	index, err := strconv.ParseUint(stm.args[1], 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse id: %s", err.Error())
	}
	n, err := stm.row.Save(uint32(index))
	if err != nil {
		log.Printf("Failed to run insert: %s", err.Error())
		return
	}
	log.Printf("Inserted %d bytes to table %s\n", n, stm.row.Table().String())
}
