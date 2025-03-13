package main

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type Where struct {
	ColumnName     string
	ValueToCompare []byte
	Operator       string
}

type SelectStatementResult struct {
	TableName  string
	Columns    []string
	AllColumns bool
	Where      *Where
}

func parseSelectStatement(stmt sqlparser.Statement) (*SelectStatementResult, error) {
	tableNameOut := ""

	columns := []string{}
	allColumns := false

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not a select statement")
	}
	for _, expr := range selectStmt.SelectExprs {
		if col, ok := expr.(*sqlparser.AliasedExpr); ok {
			columns = append(columns, sqlparser.String(col.Expr))
		} else if _, ok := expr.(*sqlparser.StarExpr); ok {
			allColumns = true
		} else {
			return nil, fmt.Errorf("could not extract columns from select statement")
		}
	}

	for _, tableExpr := range selectStmt.From {
		if aliased, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := aliased.Expr.(sqlparser.TableName); ok {
				tableNameOut = tableName.Name.String()
			} else {
				return nil, fmt.Errorf("table name not present in select statement")
			}
		} else {
			return nil, fmt.Errorf("could not extract table name alias")
		}
	}

	var where Where

	if selectStmt.Where != nil {
		compare, ok := selectStmt.Where.Expr.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, fmt.Errorf("can only do comparison rn")
		}
		where = Where{}
		if val, ok := extractSQLLiteralValue(compare.Left); ok {
			where.ValueToCompare = val
		} else {
			where.ColumnName = sqlparser.String(compare.Left)
		}
		if val, ok := extractSQLLiteralValue(compare.Right); ok {
			if where.ValueToCompare != nil {
				return nil, fmt.Errorf("only column to value compare allowed")
			}
			where.ValueToCompare = val
		} else {
			if len(where.ColumnName) > 0 {
				return nil, fmt.Errorf("only column to value compare allowed")
			}
			where.ColumnName = strings.ToLower(sqlparser.String(compare.Left))
		}

		if compare.Operator != "=" {
			return nil, fmt.Errorf("only = operator allowed")
		}
	}

	return &SelectStatementResult{
		TableName:  tableNameOut,
		Columns:    columns,
		AllColumns: allColumns,
		Where:      &where,
	}, nil
}

type Column struct {
	Name    string
	Type    string
	IsRowId bool
}

type TableSchema struct {
	TableName string
	Columns   []Column
}

func parseTableSchema(query string) (*TableSchema, error) {
	query = preprocessSQL(query)
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	ddlStmt, ok := stmt.(*sqlparser.DDL)
	if !ok || ddlStmt.Action != "create" {
		return nil, fmt.Errorf("not a ddl statement")
	}

	tableName := ddlStmt.Table.Name.String()

	columns := []Column{}

	for _, col := range ddlStmt.TableSpec.Columns {
		columns = append(columns, Column{
			Name:    strings.ToLower(col.Name.String()),
			Type:    col.Type.Type,
			IsRowId: col.Type.KeyOpt == 1 && col.Type.Type == "integer",
		})
	}

	return &TableSchema{
		TableName: tableName,
		Columns:   columns,
	}, nil
}

func preprocessSQL(sql string) string {
	reAutoInc := regexp.MustCompile(`(?i)\b autoincrement\b`)
	sql = reAutoInc.ReplaceAllString(sql, "")

	reDoubleQuotes := regexp.MustCompile(`"([^"]+)"`) // Match `"table_name"` and replace with `table_name`
	sql = reDoubleQuotes.ReplaceAllString(sql, "`$1`")

	return sql
}

func extractSQLLiteralValue(expr sqlparser.Expr) ([]byte, bool) {
	if sqlVal, ok := expr.(*sqlparser.SQLVal); ok {
		return sqlVal.Val, true
	}
	return nil, false
}
