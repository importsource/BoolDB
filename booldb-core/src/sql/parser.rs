use sqlparser::ast::{self, ColumnOption, DataType as SqlDataType, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{BoolDBError, Result};
use crate::types::{Column, DataType};

/// Parse a SQL string into sqlparser AST statements.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| BoolDBError::Parse(e.to_string()))
}

/// Convert sqlparser DataType to our DataType.
pub fn convert_data_type(sql_type: &SqlDataType) -> Result<DataType> {
    match sql_type {
        SqlDataType::Int(_)
        | SqlDataType::Integer(_)
        | SqlDataType::BigInt(_)
        | SqlDataType::SmallInt(_)
        | SqlDataType::TinyInt(_) => Ok(DataType::Integer),

        SqlDataType::Float(_)
        | SqlDataType::Double
        | SqlDataType::DoublePrecision
        | SqlDataType::Real => Ok(DataType::Float),

        SqlDataType::Varchar(_)
        | SqlDataType::Char(_)
        | SqlDataType::Text
        | SqlDataType::String(_) => Ok(DataType::Text),

        SqlDataType::Boolean => Ok(DataType::Boolean),

        SqlDataType::JSON => Ok(DataType::Json),

        other => Err(BoolDBError::Parse(format!(
            "Unsupported data type: {:?}",
            other
        ))),
    }
}

/// Convert a sqlparser column definition to our Column type.
pub fn convert_column_def(col: &ast::ColumnDef) -> Result<Column> {
    let data_type = convert_data_type(&col.data_type)?;
    let mut nullable = true;
    let mut primary_key = false;

    for option in &col.options {
        match &option.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Null => nullable = true,
            ColumnOption::Unique { is_primary, .. } => {
                if *is_primary {
                    primary_key = true;
                    nullable = false;
                }
            }
            _ => {}
        }
    }

    Ok(Column {
        name: col.name.value.clone(),
        data_type,
        nullable,
        primary_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_table() {
        let stmts = parse_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER NOT NULL)",
        )
        .unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::CreateTable { name, columns, .. } => {
                assert_eq!(name.to_string(), "users");
                assert_eq!(columns.len(), 3);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmts =
            parse_sql("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Insert { .. }));
    }

    #[test]
    fn test_parse_select() {
        let stmts = parse_sql("SELECT * FROM users").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Query(_)));
    }

    #[test]
    fn test_convert_data_type() {
        assert_eq!(
            convert_data_type(&SqlDataType::Integer(None)).unwrap(),
            DataType::Integer
        );
        assert_eq!(
            convert_data_type(&SqlDataType::Text).unwrap(),
            DataType::Text
        );
        assert_eq!(
            convert_data_type(&SqlDataType::Boolean).unwrap(),
            DataType::Boolean
        );
    }
}
