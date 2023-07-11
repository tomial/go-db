package datatype

import "encoding/binary"

const StringSize uint32 = 255
const Int64Size uint32 = binary.MaxVarintLen64
const Uint64Size uint32 = binary.MaxVarintLen64

var dataTypeSize = map[string]uint32{
	"string": StringSize,
	"int":    Int64Size,
	"uint":   Uint64Size,
}
