package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/genji1037/cockroach_sst_resolve/types"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var inputFilePath, outputFilePath string
var tableMapping map[int]*types.TableMeta // mapping table no an table metadata

func init() {
	rootCmd.AddCommand(resolveCmd)
	resolveCmd.Flags().StringVarP(&inputFilePath, "filePath", "f", "", "import file path required")
	resolveCmd.Flags().StringVarP(&outputFilePath, "output", "o", "", "output file path required")
	tableMapping = initKVSqlMapping()
}

func initKVSqlMapping() map[int]*types.TableMeta {
	tableMapping := map[int]*types.TableMeta{
		60: {
			TableName:    "moments",
			LineNum:      57,
			ColumnsIndex: []int{4, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56},
			RowTemplate:  "(%s, '%s', '%s', %s, %s, '%s', '%s', '%s', %s, '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', %s, %s)",
		},
	}
	return tableMapping
}

var resolveCmd = &cobra.Command{
	Use:   "kv",
	Short: "translate human readable cockroach kv to sql",
	Long:  `translate human readable cockroach kv to sql`,
	Run: func(cmd *cobra.Command, args []string) {

		beginTs := time.Now()

		inputFile, err := os.OpenFile(inputFilePath, 0, 0644)
		if err != nil {
			fmt.Printf("open file [%s] failed: %s\n", inputFilePath, err.Error())
			return
		}
		outputFile, err := os.OpenFile(outputFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Printf("open or create file [%s] failed: %s\n", inputFilePath, err.Error())
			return
		}

		bufReader := bufio.NewReader(inputFile)
		lineNo := 0

		sqlBufs := make(map[string]*types.SqlBuf)

		for {

			lineNo++

			line, isPrefix, err := bufReader.ReadLine() // 按行读
			for isPrefix {
				oldLine := string(line)
				var newLine []byte
				newLine, isPrefix, err = bufReader.ReadLine()
				if err != nil {
					if err == io.EOF {
						err = nil
						break
					}
				} else {
					line = []byte(oldLine + string(newLine))
				}
			}

			if err != nil {
				if err == io.EOF {
					err = nil
					break
				}
			} else {
				tmpArr := strings.Split(string(line), "/")
				if len(tmpArr) < 8 {
					fmt.Printf("[%d] len [%d] to small.\n", lineNo, len(tmpArr))
					continue
				}
				tableNoStr := tmpArr[2]
				tableNo, err := strconv.ParseInt(tableNoStr, 10, 64)
				if err != nil {
					fmt.Printf("[%d] convert [%s] to table no failed: %s.\n", lineNo, tableNoStr, err.Error())
					continue
				}
				tableMeta, ok := tableMapping[int(tableNo)]
				if !ok {
					fmt.Printf("[%d] unknown table no [%d] skip it.\n", lineNo, tableNo)
					continue
				}

				// prehandle
				if tmpArr[7] == "??? => " {

				} else if tmpArr[9] == "??? => " {
					overLen := 2
					for i := 7 + overLen; i < len(tmpArr); i++ {
						tmpArr[i-overLen] = tmpArr[i]
					}
					tmpArr = tmpArr[:len(tmpArr)-overLen]
				} else if tmpArr[8] == "TUPLE" {

				} else {
					overLen := 1
					for i := 7 + overLen; i < len(tmpArr); i++ {
						tmpArr[i-overLen] = tmpArr[i]
					}
					tmpArr = tmpArr[:len(tmpArr)-overLen]
				}

				// moments prehandle
				if tableMeta.TableName == "moments" {
					if len(tmpArr) > tableMeta.LineNum {
						overLen := len(tmpArr) - tableMeta.LineNum // 朋友圈内容含有/的长度会超出标准长度
						momentContent := ""
						for i := 0; i <= overLen; i++ {
							momentContent = momentContent + tmpArr[12+i] + "/"
						}
						tmpArr[12] = momentContent[:len(momentContent)-1]
						for i := 13 + overLen; i < len(tmpArr); i++ {
							tmpArr[i-overLen] = tmpArr[i]
						}
						tmpArr = tmpArr[:len(tmpArr)-overLen]
					}
				}

				// validate line num
				if len(tmpArr) != tableMeta.LineNum {
					fmt.Printf("[%d] expect lineNo [%d] actual lineNo[%d].\n", lineNo, tableMeta.LineNum, len(tmpArr))
					continue
				}

				// build insert sql
				values := make([]interface{}, 0)
				for _, index := range tableMeta.ColumnsIndex {

					if strings.ContainsAny(tmpArr[index], "'") {
						tmpArr[index] = strings.ReplaceAll(tmpArr[index], "'", "''")
					}

					values = append(values, tmpArr[index])

				}

				sqlRowPart := fmt.Sprintf(tableMeta.RowTemplate, values...)

				// insert into sql buf
				sqlBuf, ok := sqlBufs[tableMeta.TableName]
				if !ok {
					sqlBuf = &types.SqlBuf{
						TableName: tableMeta.TableName,
						TableNo:   int(tableNo),
						Buf:       bytes.Buffer{},
						RowNum:    0,
						PKs:       make(map[string]struct{}),
					}
					sqlBufs[tableMeta.TableName] = sqlBuf
				}

				_, ok = sqlBuf.PKs[tmpArr[4]]
				if ok { // 主键冲突
					fmt.Printf("pk [%s] conflict skip it.\n", tmpArr[4])
					continue
				}
				sqlBuf.PKs[tmpArr[4]] = struct{}{}

				if sqlBuf.RowNum > 0 {
					sqlBuf.Buf.WriteString(",\n")
				}
				sqlBuf.Buf.WriteString(sqlRowPart)
				sqlBuf.RowNum++

				// check rowNum if over 50
				if sqlBuf.RowNum >= 50 {
					_, err := outputFile.WriteString("insert into " + sqlBuf.TableName + " values \n" + sqlBuf.Buf.String() + ";\n")
					if err != nil {
						panic(err)
					}
					sqlBuf.RowNum = 0
					sqlBuf.Buf = bytes.Buffer{}
				}

			}

		}

		// flush all sqlbufs
		for _, sqlBuf := range sqlBufs {
			if sqlBuf.RowNum == 0 {
				continue
			}
			_, err := outputFile.WriteString("insert into " + sqlBuf.TableName + " values \n" + sqlBuf.Buf.String() + ";\n")
			if err != nil {
				panic(err)
			}
		}

		fmt.Printf("resloved %d lines, cost %s.\n", lineNo, time.Now().Sub(beginTs))

	},
}
