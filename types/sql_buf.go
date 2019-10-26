package types

import "bytes"

type SqlBuf struct {
	TableName string
	TableNo   int
	Buf       bytes.Buffer
	RowNum    int
}
