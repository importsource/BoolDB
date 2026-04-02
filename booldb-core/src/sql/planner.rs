use sqlparser::ast::{
    self, Expr, JoinConstraint, JoinOperator, SelectItem, SetExpr, Statement, TableFactor,
    Value as SqlValue,
};

use crate::error::{BoolDBError, Result};
use crate::sql::parser::{convert_column_def, parse_sql};
use crate::types::{Column, Row, Schema, Value};

/// A logical plan node representing a query or command.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    CreateTable {
        schema: Schema,
    },
    DropTable {
        table_name: String,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        rows: Vec<Row>,
    },
    Select {
        table_name: String,
        projection: Projection,
        filter: Option<FilterExpr>,
        joins: Vec<JoinClause>,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, Value)>,
        filter: Option<FilterExpr>,
    },
    Delete {
        table_name: String,
        filter: Option<FilterExpr>,
    },
}

/// An expression in a SELECT projection.
#[derive(Debug, Clone)]
pub enum SelectExpr {
    Column(String),
    JsonExtract { column: String, path: String },
}

/// What columns to return.
#[derive(Debug, Clone)]
pub enum Projection {
    All,
    Expressions(Vec<SelectExpr>),
}

/// A filter expression (WHERE clause).
#[derive(Debug, Clone)]
pub enum FilterExpr {
    Comparison {
        column: String,
        op: CmpOp,
        value: Value,
    },
    JsonExtract {
        column: String,
        path: String,
        op: CmpOp,
        value: Value,
    },
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
    IsNull(String),
    IsNotNull(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// A join clause.
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table_name: String,
    pub left_col: String,
    pub right_col: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// Plan a SQL string into a LogicalPlan.
pub fn plan(sql: &str) -> Result<LogicalPlan> {
    let stmts = parse_sql(sql)?;
    if stmts.is_empty() {
        return Err(BoolDBError::Sql("Empty SQL statement".to_string()));
    }
    if stmts.len() > 1 {
        return Err(BoolDBError::Sql(
            "Only single statements are supported".to_string(),
        ));
    }
    plan_statement(&stmts[0])
}

fn plan_statement(stmt: &Statement) -> Result<LogicalPlan> {
    match stmt {
        Statement::CreateTable {
            name,
            columns,
            if_not_exists,
            ..
        } => plan_create_table(name, columns, *if_not_exists),
        Statement::Drop { names, .. } => plan_drop_table(names),
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => plan_insert(table_name, columns, source),
        Statement::Query(query) => plan_query(query),
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => plan_update(table, assignments, selection),
        Statement::Delete {
            from, selection, ..
        } => plan_delete_from(from, selection),
        _ => Err(BoolDBError::Sql(format!(
            "Unsupported statement: {:?}",
            stmt
        ))),
    }
}

fn plan_create_table(
    name: &ast::ObjectName,
    columns: &[ast::ColumnDef],
    _if_not_exists: bool,
) -> Result<LogicalPlan> {
    let table_name = name.to_string();
    let cols: Vec<Column> = columns
        .iter()
        .map(convert_column_def)
        .collect::<Result<Vec<_>>>()?;

    Ok(LogicalPlan::CreateTable {
        schema: Schema {
            table_name,
            columns: cols,
        },
    })
}

fn plan_drop_table(names: &[ast::ObjectName]) -> Result<LogicalPlan> {
    if names.len() != 1 {
        return Err(BoolDBError::Sql(
            "DROP TABLE supports exactly one table".to_string(),
        ));
    }
    Ok(LogicalPlan::DropTable {
        table_name: names[0].to_string(),
    })
}

fn plan_insert(
    table_name: &ast::ObjectName,
    columns: &[ast::Ident],
    source: &Option<Box<ast::Query>>,
) -> Result<LogicalPlan> {
    let source = source
        .as_ref()
        .ok_or_else(|| BoolDBError::Sql("INSERT requires VALUES".to_string()))?;

    let col_names = if columns.is_empty() {
        None
    } else {
        Some(columns.iter().map(|c| c.value.clone()).collect())
    };

    let rows = match source.body.as_ref() {
        SetExpr::Values(values) => {
            let mut result = Vec::new();
            for row_exprs in &values.rows {
                let row: Row = row_exprs
                    .iter()
                    .map(convert_expr_to_value)
                    .collect::<Result<Vec<_>>>()?;
                result.push(row);
            }
            result
        }
        _ => {
            return Err(BoolDBError::Sql(
                "INSERT only supports VALUES clause".to_string(),
            ))
        }
    };

    Ok(LogicalPlan::Insert {
        table_name: table_name.to_string(),
        columns: col_names,
        rows,
    })
}

fn plan_query(query: &ast::Query) -> Result<LogicalPlan> {
    match query.body.as_ref() {
        SetExpr::Select(select) => plan_select(select),
        _ => Err(BoolDBError::Sql(
            "Only simple SELECT queries are supported".to_string(),
        )),
    }
}

fn plan_select(select: &ast::Select) -> Result<LogicalPlan> {
    if select.from.is_empty() {
        return Err(BoolDBError::Sql("SELECT requires FROM clause".to_string()));
    }

    let table_name = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(BoolDBError::Sql(
                "Only simple table names are supported".to_string(),
            ))
        }
    };

    // Parse projection
    let projection = if select.projection.len() == 1
        && matches!(&select.projection[0], SelectItem::Wildcard(_))
    {
        Projection::All
    } else {
        let exprs: Vec<SelectExpr> = select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    Ok(SelectExpr::Column(ident.value.clone()))
                }
                SelectItem::UnnamedExpr(expr) => parse_select_expr(expr),
                SelectItem::Wildcard(_) => Err(BoolDBError::Sql(
                    "Wildcard must be the only projection item".to_string(),
                )),
                _ => Err(BoolDBError::Sql(format!(
                    "Unsupported projection: {:?}",
                    item
                ))),
            })
            .collect::<Result<Vec<_>>>()?;
        Projection::Expressions(exprs)
    };

    // Parse WHERE clause
    let filter = match &select.selection {
        Some(expr) => Some(convert_expr_to_filter(expr)?),
        None => None,
    };

    // Parse JOINs
    let mut joins = Vec::new();
    for join in &select.from[0].joins {
        let right_table = match &join.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => {
                return Err(BoolDBError::Sql(
                    "Only simple table joins supported".to_string(),
                ))
            }
        };

        let join_type = match &join.join_operator {
            JoinOperator::Inner(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) => JoinType::Left,
            JoinOperator::RightOuter(_) => JoinType::Right,
            _ => {
                return Err(BoolDBError::Sql(format!(
                    "Unsupported join type: {:?}",
                    join.join_operator
                )))
            }
        };

        let (left_col, right_col) = match &join.join_operator {
            JoinOperator::Inner(constraint)
            | JoinOperator::LeftOuter(constraint)
            | JoinOperator::RightOuter(constraint) => extract_join_cols(constraint)?,
            _ => {
                return Err(BoolDBError::Sql("Unsupported join constraint".to_string()))
            }
        };

        joins.push(JoinClause {
            join_type,
            table_name: right_table,
            left_col,
            right_col,
        });
    }

    Ok(LogicalPlan::Select {
        table_name,
        projection,
        filter,
        joins,
    })
}

fn plan_update(
    table: &ast::TableWithJoins,
    assignments: &[ast::Assignment],
    selection: &Option<Expr>,
) -> Result<LogicalPlan> {
    let table_name = match &table.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err(BoolDBError::Sql("Invalid UPDATE target".to_string())),
    };

    let mut assigns = Vec::new();
    for a in assignments {
        let col_name = a
            .id
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join(".");
        let value = convert_expr_to_value(&a.value)?;
        assigns.push((col_name, value));
    }

    let filter = match selection {
        Some(expr) => Some(convert_expr_to_filter(expr)?),
        None => None,
    };

    Ok(LogicalPlan::Update {
        table_name,
        assignments: assigns,
        filter,
    })
}

fn plan_delete_from(
    from: &Vec<ast::TableWithJoins>,
    selection: &Option<Expr>,
) -> Result<LogicalPlan> {
    if from.is_empty() {
        return Err(BoolDBError::Sql("DELETE requires FROM".to_string()));
    }
    let table_name = match &from[0].relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err(BoolDBError::Sql("Invalid DELETE target".to_string())),
    };

    let filter = match selection {
        Some(expr) => Some(convert_expr_to_filter(expr)?),
        None => None,
    };

    Ok(LogicalPlan::Delete {
        table_name,
        filter,
    })
}

/// Convert an AST expression to a runtime Value.
pub fn convert_expr_to_value(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(SqlValue::Number(n, _)) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Value::Integer(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(Value::Float(f))
            } else {
                Err(BoolDBError::Parse(format!("Invalid number: {}", n)))
            }
        }
        Expr::Value(SqlValue::SingleQuotedString(s))
        | Expr::Value(SqlValue::DoubleQuotedString(s)) => Ok(Value::Text(s.clone())),
        Expr::Value(SqlValue::Boolean(b)) => Ok(Value::Boolean(*b)),
        Expr::Value(SqlValue::Null) => Ok(Value::Null),
        Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr,
        } => match convert_expr_to_value(expr)? {
            Value::Integer(i) => Ok(Value::Integer(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            _ => Err(BoolDBError::Parse(
                "Unary minus on non-numeric".to_string(),
            )),
        },
        _ => Err(BoolDBError::Parse(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

/// Convert a WHERE expression AST node to our FilterExpr.
/// Parse a SELECT expression (column or json_extract call).
fn parse_select_expr(expr: &Expr) -> Result<SelectExpr> {
    if let Some((col, path)) = try_parse_json_extract(expr) {
        return Ok(SelectExpr::JsonExtract { column: col, path });
    }
    match expr {
        Expr::Identifier(ident) => Ok(SelectExpr::Column(ident.value.clone())),
        _ => Err(BoolDBError::Sql(format!(
            "Unsupported select expression: {:?}",
            expr
        ))),
    }
}

/// Try to parse `json_extract(column, '$.path')` from an AST expression.
fn try_parse_json_extract(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();
            if name != "json_extract" {
                return None;
            }
            let args = &func.args;
            if args.len() != 2 {
                return None;
            }
            let col = match &args[0] {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                    ident.value.clone()
                }
                _ => return None,
            };
            let path = match &args[1] {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(Expr::Value(
                    SqlValue::SingleQuotedString(s),
                ))) => s.clone(),
                _ => return None,
            };
            Some((col, path))
        }
        _ => None,
    }
}

fn convert_expr_to_filter(expr: &Expr) -> Result<FilterExpr> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Check if this is AND/OR first
            match op {
                ast::BinaryOperator::And => {
                    let l = convert_expr_to_filter(left)?;
                    let r = convert_expr_to_filter(right)?;
                    return Ok(FilterExpr::And(Box::new(l), Box::new(r)));
                }
                ast::BinaryOperator::Or => {
                    let l = convert_expr_to_filter(left)?;
                    let r = convert_expr_to_filter(right)?;
                    return Ok(FilterExpr::Or(Box::new(l), Box::new(r)));
                }
                _ => {}
            }

            let cmp_op = match op {
                ast::BinaryOperator::Eq => CmpOp::Eq,
                ast::BinaryOperator::NotEq => CmpOp::NotEq,
                ast::BinaryOperator::Lt => CmpOp::Lt,
                ast::BinaryOperator::LtEq => CmpOp::LtEq,
                ast::BinaryOperator::Gt => CmpOp::Gt,
                ast::BinaryOperator::GtEq => CmpOp::GtEq,
                _ => {
                    return Err(BoolDBError::Sql(format!(
                        "Unsupported operator: {:?}",
                        op
                    )))
                }
            };

            let value = convert_expr_to_value(right)?;

            // Check if left side is json_extract(col, '$.path')
            if let Some((col, path)) = try_parse_json_extract(left) {
                return Ok(FilterExpr::JsonExtract {
                    column: col,
                    path,
                    op: cmp_op,
                    value,
                });
            }

            // Regular column comparison
            let column = match left.as_ref() {
                Expr::Identifier(ident) => ident.value.clone(),
                _ => {
                    return Err(BoolDBError::Sql(format!(
                        "Left side of comparison must be a column name or json_extract(), got: {:?}",
                        left
                    )))
                }
            };

            Ok(FilterExpr::Comparison {
                column,
                op: cmp_op,
                value,
            })
        }
        Expr::IsNull(inner) => match inner.as_ref() {
            Expr::Identifier(ident) => Ok(FilterExpr::IsNull(ident.value.clone())),
            _ => Err(BoolDBError::Sql("IS NULL requires column name".to_string())),
        },
        Expr::IsNotNull(inner) => match inner.as_ref() {
            Expr::Identifier(ident) => Ok(FilterExpr::IsNotNull(ident.value.clone())),
            _ => Err(BoolDBError::Sql(
                "IS NOT NULL requires column name".to_string(),
            )),
        },
        Expr::UnaryOp {
            op: ast::UnaryOperator::Not,
            expr,
        } => {
            let inner = convert_expr_to_filter(expr)?;
            Ok(FilterExpr::Not(Box::new(inner)))
        }
        _ => Err(BoolDBError::Sql(format!(
            "Unsupported filter expression: {:?}",
            expr
        ))),
    }
}

fn extract_join_cols(constraint: &JoinConstraint) -> Result<(String, String)> {
    match constraint {
        JoinConstraint::On(Expr::BinaryOp { left, op, right }) => {
            if !matches!(op, ast::BinaryOperator::Eq) {
                return Err(BoolDBError::Sql(
                    "JOIN ON must use = operator".to_string(),
                ));
            }
            let left_col = match left.as_ref() {
                Expr::CompoundIdentifier(parts) => parts.last().unwrap().value.clone(),
                Expr::Identifier(ident) => ident.value.clone(),
                _ => {
                    return Err(BoolDBError::Sql(
                        "JOIN ON left side must be column".to_string(),
                    ))
                }
            };
            let right_col = match right.as_ref() {
                Expr::CompoundIdentifier(parts) => parts.last().unwrap().value.clone(),
                Expr::Identifier(ident) => ident.value.clone(),
                _ => {
                    return Err(BoolDBError::Sql(
                        "JOIN ON right side must be column".to_string(),
                    ))
                }
            };
            Ok((left_col, right_col))
        }
        _ => Err(BoolDBError::Sql(
            "Only ON clause supported for JOINs".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_create_table() {
        let plan = plan("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        match plan {
            LogicalPlan::CreateTable { schema } => {
                assert_eq!(schema.table_name, "users");
                assert_eq!(schema.columns.len(), 2);
                assert!(schema.columns[0].primary_key);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_plan_insert() {
        let plan = plan("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        match plan {
            LogicalPlan::Insert {
                table_name, rows, ..
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0], vec![Value::Integer(1), Value::Text("Alice".to_string())]);
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_plan_select_all() {
        let plan = plan("SELECT * FROM users").unwrap();
        match plan {
            LogicalPlan::Select {
                table_name,
                projection,
                filter,
                ..
            } => {
                assert_eq!(table_name, "users");
                assert!(matches!(projection, Projection::All));
                assert!(filter.is_none());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_plan_select_with_where() {
        let plan = plan("SELECT * FROM users WHERE age > 25").unwrap();
        match plan {
            LogicalPlan::Select { filter, .. } => {
                assert!(filter.is_some());
                match filter.unwrap() {
                    FilterExpr::Comparison { column, op, value } => {
                        assert_eq!(column, "age");
                        assert_eq!(op, CmpOp::Gt);
                        assert_eq!(value, Value::Integer(25));
                    }
                    _ => panic!("Expected Comparison"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_plan_update() {
        let plan = plan("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        match plan {
            LogicalPlan::Update {
                table_name,
                assignments,
                filter,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].0, "name");
                assert!(filter.is_some());
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_plan_delete() {
        let plan = plan("DELETE FROM users WHERE id = 1").unwrap();
        match plan {
            LogicalPlan::Delete {
                table_name,
                filter,
            } => {
                assert_eq!(table_name, "users");
                assert!(filter.is_some());
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_plan_drop_table() {
        let plan = plan("DROP TABLE users").unwrap();
        match plan {
            LogicalPlan::DropTable { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected DropTable"),
        }
    }
}
