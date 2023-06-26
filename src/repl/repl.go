package repl

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func Run() {
	for {
		fmt.Print("db > ")

		reader := bufio.NewReader(os.Stdin)

		str, err := reader.ReadString('\n')
		if err != nil {
			os.Exit(1)
		}

		// \n was included in reader.ReadString
		// TODO Handle complex invalid input
		str = strings.TrimSpace(str)

		ib := inputBuffer{args: strings.Split(str, " ")}

		op := ib.args[0]
		if len(op) == 0 {
			continue
		}
		if op[0] == '.' {
			executeMetaCmd(&ib)
		} else {
			stm := statement{}
			prepareStatus := prepareStm(&ib, &stm)
			if prepareStatus == PrepareStatementFailed {
				fmt.Printf("Failed to prepare statement: %v\n", ib.args)
				continue
			}
			stm.Execute()
			fmt.Println("Executed")
		}
	}
}
