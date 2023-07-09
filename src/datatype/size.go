package datatype

import "encoding/binary"

const StringSize uint = 255
const Int64Size uint = binary.MaxVarintLen64
const Uint64Size uint = binary.MaxVarintLen64

var dataTypeSize = map[string]uint{
	"string": StringSize,
	"int":    Int64Size,
	"uint":   Uint64Size,
}
