package util

// advance index cursor

type Number interface {
	uint | int | uint32 | uint8
}

func AdvanceCursor[N Number](num N, amount N) N {
	return num + amount
}
