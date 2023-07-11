package row

import (
	"db/src/datatype"
	"encoding/binary"
	"errors"
	"reflect"
)

// TODO return when data count not equal to col num
func serialize(row Row, columnSize uint32) (data []byte, err error) {
	val := reflect.ValueOf(row)

	buf := make([]byte, columnSize)
	var pos uint32 = 0

	for i := 0; i < val.Elem().NumField(); i++ {
		fieldType := val.Elem().Field(i).Kind()

		if fieldType != reflect.Struct && fieldType != reflect.Interface {
			switch fieldType {
			case reflect.Uint64:
				{
					binary.PutUvarint(buf, val.Elem().Field(i).Uint())
					pos += datatype.Uint64Size
				}
			case reflect.Int64:
				{
					binary.PutVarint(buf, val.Elem().Field(i).Int())
					pos += datatype.Uint64Size
				}
			case reflect.String:
				{
					copy(buf[pos:pos+255], []byte(val.Elem().Field(i).String()))
					pos += datatype.StringSize
				}
			}
		}
	}

	return buf, nil
}

func deserialize(data []byte, tableType reflect.Type) (Row, error) {
	row := reflect.New(tableType)

	var pos uint32 = 0

	for i := 0; i < tableType.NumField(); i++ {

		fieldType := row.Elem().Field(i).Kind()

		if fieldType != reflect.Struct && fieldType != reflect.Interface {
			switch fieldType {
			case reflect.Uint64:
				{
					udigit, n := binary.Uvarint(data[pos : pos+datatype.Uint64Size])
					if n <= 0 {
						return nil, errors.New("deserializing data: invalid size byte slice, too small or too large")
					}
					row.Elem().Field(i).SetUint(udigit)
					pos += datatype.Uint64Size
				}
			case reflect.Int64:
				{
					digit, n := binary.Varint(data[pos : pos+datatype.Uint64Size])
					if n <= 0 {
						return nil, errors.New("deserializing data: invalid size byte slice, too small or too large")
					}
					row.Elem().Field(i).SetInt(digit)
					pos += datatype.Int64Size
				}
			case reflect.String:
				{
					row.Elem().Field(i).SetString(string(data[pos : pos+datatype.StringSize]))
					pos += datatype.StringSize
				}
			}
		}
	}

	return row.Interface().(Row), nil
}
