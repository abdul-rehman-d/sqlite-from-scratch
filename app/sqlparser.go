package main

import (
	"fmt"
	"regexp"

	"github.com/xwb1989/sqlparser"
)

type SelectStatementResult struct {
	TableName string
	Columns   []string
}

func parseSelectStatement(stmt sqlparser.Statement) (*SelectStatementResult, error) {
	tableNameOut := ""

	columns := []string{}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not a select statement")
	}
	for _, expr := range selectStmt.SelectExprs {
		if col, ok := expr.(*sqlparser.AliasedExpr); ok {
			columns = append(columns, sqlparser.String(col.Expr))
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

	return &SelectStatementResult{
		TableName: tableNameOut,
		Columns:   columns,
	}, nil
}

type Column struct {
	Name string
	Type string
}

type TableSchema struct {
	TableName string
	Columns   []Column
}

func parseTableSchema(query string) (*TableSchema, error) {
	stmt, err := sqlparser.Parse(preprocessSQL(query))
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
			Name: strings.ToLower(col.Name.String()),
			Type: col.Type.Type,
		})
	}

	return &TableSchema{
		TableName: tableName,
		Columns:   columns,
	}, nil
}

func preprocessSQL(sql string) string {
	re := regexp.MustCompile(`(?i)\bautoincrement\b`) // Match 'AUTOINCREMENT' case-insensitively
	return re.ReplaceAllString(sql, "")
}
