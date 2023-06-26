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
		op := strings.TrimSuffix(str, "\n")

		if op == ".exit" {
			os.Exit(0)
		} else {
			fmt.Printf("Unrecognized operation: %s\n", op)
		}
	}
}
