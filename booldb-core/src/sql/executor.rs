use crate::error::{BoolDBError, Result};
use crate::sql::json::json_extract;
use crate::sql::planner::{CmpOp, FilterExpr, JoinClause, JoinType, LogicalPlan, Projection, SelectExpr};
use crate::storage::buffer::BufferPool;
use crate::storage::heap::HeapFile;
use crate::catalog::schema::Catalog;
use crate::types::{Row, Schema, Tuple, Value};

/// Result of executing a SQL statement.
#[derive(Debug)]
pub enum ExecResult {
    /// DDL result (CREATE TABLE, DROP TABLE).
    Ok { message: String },
    /// Number of rows affected (INSERT, UPDATE, DELETE).
    RowsAffected { count: usize },
    /// Query result (SELECT).
    Rows {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
}

/// Execute a logical plan against the database.
pub fn execute(
    plan: &LogicalPlan,
    catalog: &mut Catalog,
    heaps: &mut std::collections::HashMap<String, HeapFile>,
    pool: &mut BufferPool,
) -> Result<ExecResult> {
    match plan {
        LogicalPlan::CreateTable { schema } => exec_create_table(schema, catalog, heaps),
        LogicalPlan::DropTable { table_name } => exec_drop_table(table_name, catalog, heaps),
        LogicalPlan::Insert {
            table_name,
            columns,
            rows,
        } => exec_insert(table_name, columns, rows, catalog, heaps, pool),
        LogicalPlan::Select {
            table_name,
            projection,
            filter,
            joins,
        } => exec_select(table_name, projection, filter, joins, catalog, heaps, pool),
        LogicalPlan::Update {
            table_name,
            assignments,
            filter,
        } => exec_update(table_name, assignments, filter, catalog, heaps, pool),
        LogicalPlan::Delete {
            table_name,
            filter,
        } => exec_delete(table_name, filter, catalog, heaps, pool),
    }
}

fn exec_create_table(
    schema: &Schema,
    catalog: &mut Catalog,
    heaps: &mut std::collections::HashMap<String, HeapFile>,
) -> Result<ExecResult> {
    catalog.create_table(schema.clone())?;
    heaps.insert(
        schema.table_name.clone(),
        HeapFile::new(&schema.table_name),
    );
    Ok(ExecResult::Ok {
        message: format!("Table '{}' created", schema.table_name),
    })
}

fn exec_drop_table(
    table_name: &str,
    catalog: &mut Catalog,
    heaps: &mut std::collections::HashMap<String, HeapFile>,
) -> Result<ExecResult> {
    catalog.drop_table(table_name)?;
    heaps.remove(table_name);
    Ok(ExecResult::Ok {
        message: format!("Table '{}' dropped", table_name),
    })
}

fn exec_insert(
    table_name: &str,
    columns: &Option<Vec<String>>,
    rows: &[Row],
    catalog: &mut Catalog,
    heaps: &mut std::collections::HashMap<String, HeapFile>,
    pool: &mut BufferPool,
) -> Result<ExecResult> {
    let schema = catalog.get_table(table_name)?.schema.clone();
    let heap = heaps
        .get_mut(table_name)
        .ok_or_else(|| BoolDBError::TableNotFound(table_name.to_string()))?;

    let mut count = 0;
    for row in rows {
        let ordered_row = if let Some(col_names) = columns {
            // Reorder values to match schema column order.
            let mut ordered = vec![Value::Null; schema.columns.len()];
            for (i, col_name) in col_names.iter().enumerate() {
                let idx = schema.column_index(col_name).ok_or_else(|| {
                    BoolDBError::ColumnNotFound(col_name.clone())
                })?;
                if i < row.len() {
                    ordered[idx] = row[i].clone();
                }
            }
            ordered
        } else {
            if row.len() != schema.columns.len() {
                return Err(BoolDBError::Sql(format!(
                    "Expected {} values, got {}",
                    schema.columns.len(),
                    row.len()
                )));
            }
            row.clone()
        };

        heap.insert(pool, &ordered_row)?;
        count += 1;
    }

    // Update heap page IDs in catalog.
    let table_meta = catalog.get_table_mut(table_name)?;
    table_meta.heap_page_ids = heap.page_ids().to_vec();

    Ok(ExecResult::RowsAffected { count })
}

fn exec_select(
    table_name: &str,
    projection: &Projection,
    filter: &Option<FilterExpr>,
    joins: &[JoinClause],
    catalog: &mut Catalog,
    heaps: &mut std::collections::HashMap<String, HeapFile>,
    pool: &mut BufferPool,
) -> Result<ExecResult> {
    let schema = catalog.get_table(table_name)?.schema.clone();
    let heap = heaps
        .get(table_name)
        .ok_or_else(|| BoolDBError::TableNotFound(table_name.to_string()))?;

    let tuples = heap.scan(pool)?;

    // Apply JOINs
    let (joined_rows, joined_schema) = if joins.is_empty() {
        let rows: Vec<Row> = tuples.into_iter().map(|t| t.values).collect();
        (rows, schema.clone())
    } else {
        let mut current_rows: Vec<Row> = tuples.into_iter().map(|t| t.values).collect();
        let mut current_schema = schema.clone();

        for join in joins {
            let right_schema = catalog.get_table(&join.table_name)?.schema.clone();
            let right_heap = heaps
                .get(&join.table_name)
                .ok_or_else(|| BoolDBError::TableNotFound(join.table_name.clone()))?;
            let right_tuples = right_heap.scan(pool)?;

            let left_col_idx = current_schema
                .column_index(&join.left_col)
                .ok_or_else(|| BoolDBError::ColumnNotFound(join.left_col.clone()))?;
            let right_col_idx = right_schema
                .column_index(&join.right_col)
                .ok_or_else(|| BoolDBError::ColumnNotFound(join.right_col.clone()))?;

            let mut new_rows = Vec::new();

            match join.join_type {
                JoinType::Inner => {
                    for left_row in &current_rows {
                        for right_tuple in &right_tuples {
                            if left_row[left_col_idx] == right_tuple.values[right_col_idx] {
                                let mut combined = left_row.clone();
                                combined.extend(right_tuple.values.clone());
                                new_rows.push(combined);
                            }
                        }
                    }
                }
                JoinType::Left => {
                    for left_row in &current_rows {
                        let mut matched = false;
                        for right_tuple in &right_tuples {
                            if left_row[left_col_idx] == right_tuple.values[right_col_idx] {
                                let mut combined = left_row.clone();
                                combined.extend(right_tuple.values.clone());
                                new_rows.push(combined);
                                matched = true;
                            }
                        }
                        if !matched {
                            let mut combined = left_row.clone();
                            combined.extend(vec![Value::Null; right_schema.columns.len()]);
                            new_rows.push(combined);
                        }
                    }
                }
                JoinType::Right => {
                    for right_tuple in &right_tuples {
                        let mut matched = false;
                        for left_row in &current_rows {
                            if left_row[left_col_idx] == right_tuple.values[right_col_idx] {
                                let mut combined = left_row.clone();
                                combined.extend(right_tuple.values.clone());
                                new_rows.push(combined);
                                matched = true;
                            }
                        }
                        if !matched {
                            let mut combined =
                                vec![Value::Null; current_schema.columns.len()];
                            combined.extend(right_tuple.values.clone());
                            new_rows.push(combined);
                        }
                    }
                }
            }

            // Extend schema
            let mut new_columns = current_schema.columns.clone();
            new_columns.extend(right_schema.columns.clone());
            current_schema = Schema {
                table_name: current_schema.table_name.clone(),
                columns: new_columns,
            };
            current_rows = new_rows;
        }

        (current_rows, current_schema)
    };

    // Apply WHERE filter
    let filtered: Vec<Row> = if let Some(filter) = filter {
        joined_rows
            .into_iter()
            .filter(|row| evaluate_filter(filter, row, &joined_schema))
            .collect()
    } else {
        joined_rows
    };

    // Apply projection
    let (col_names, result_rows) = match projection {
        Projection::All => {
            let names = joined_schema.columns.iter().map(|c| c.name.clone()).collect();
            (names, filtered)
        }
        Projection::Expressions(exprs) => {
            let mut names = Vec::new();
            let mut projectors: Vec<Box<dyn Fn(&Row) -> Value>> = Vec::new();

            for expr in exprs {
                match expr {
                    SelectExpr::Column(col) => {
                        let idx = joined_schema
                            .column_index(col)
                            .ok_or_else(|| BoolDBError::ColumnNotFound(col.clone()))?;
                        names.push(col.clone());
                        projectors.push(Box::new(move |row: &Row| row[idx].clone()));
                    }
                    SelectExpr::JsonExtract { column, path } => {
                        let idx = joined_schema
                            .column_index(column)
                            .ok_or_else(|| BoolDBError::ColumnNotFound(column.clone()))?;
                        let path = path.clone();
                        names.push(format!("json_extract({}, '{}')", column, path));
                        projectors.push(Box::new(move |row: &Row| {
                            match &row[idx] {
                                Value::Text(s) => json_extract(s, &path).unwrap_or(Value::Null),
                                _ => Value::Null,
                            }
                        }));
                    }
                }
            }

            let projected: Vec<Row> = filtered
                .into_iter()
                .map(|row| projectors.iter().map(|p| p(&row)).collect())
                .collect();
            (names, projected)
        }
    };

    Ok(ExecResult::Rows {
        columns: col_names,
        rows: result_rows,
    })
}

fn exec_update(
    table_name: &str,
    assignments: &[(String, Value)],
    filter: &Option<FilterExpr>,
    catalog: &mut Catalog,
    heaps: &mut std::collections::HashMap<String, HeapFile>,
    pool: &mut BufferPool,
) -> Result<ExecResult> {
    let schema = catalog.get_table(table_name)?.schema.clone();
    let heap = heaps
        .get_mut(table_name)
        .ok_or_else(|| BoolDBError::TableNotFound(table_name.to_string()))?;

    let tuples = heap.scan(pool)?;

    let mut to_update: Vec<(Tuple, Row)> = Vec::new();
    for tuple in tuples {
        let matches = match filter {
            Some(f) => evaluate_filter(f, &tuple.values, &schema),
            None => true,
        };

        if matches {
            let mut new_row = tuple.values.clone();
            for (col_name, value) in assignments {
                let idx = schema
                    .column_index(col_name)
                    .ok_or_else(|| BoolDBError::ColumnNotFound(col_name.clone()))?;
                new_row[idx] = value.clone();
            }
            to_update.push((tuple, new_row));
        }
    }

    let count = to_update.len();
    for (tuple, new_row) in to_update {
        heap.update(pool, tuple.row_id, &new_row)?;
    }

    // Update heap page IDs in catalog.
    let table_meta = catalog.get_table_mut(table_name)?;
    table_meta.heap_page_ids = heap.page_ids().to_vec();

    Ok(ExecResult::RowsAffected { count })
}

fn exec_delete(
    table_name: &str,
    filter: &Option<FilterExpr>,
    catalog: &mut Catalog,
    heaps: &mut std::collections::HashMap<String, HeapFile>,
    pool: &mut BufferPool,
) -> Result<ExecResult> {
    let schema = catalog.get_table(table_name)?.schema.clone();
    let heap = heaps
        .get_mut(table_name)
        .ok_or_else(|| BoolDBError::TableNotFound(table_name.to_string()))?;

    let tuples = heap.scan(pool)?;
    let mut to_delete = Vec::new();

    for tuple in tuples {
        let matches = match filter {
            Some(f) => evaluate_filter(f, &tuple.values, &schema),
            None => true,
        };
        if matches {
            to_delete.push(tuple.row_id);
        }
    }

    let count = to_delete.len();
    for row_id in to_delete {
        heap.delete(pool, row_id)?;
    }

    Ok(ExecResult::RowsAffected { count })
}

/// Evaluate a filter expression against a row.
pub fn evaluate_filter(filter: &FilterExpr, row: &Row, schema: &Schema) -> bool {
    match filter {
        FilterExpr::Comparison { column, op, value } => {
            let idx = match schema.column_index(column) {
                Some(i) => i,
                None => return false,
            };
            let row_val = &row[idx];
            match op {
                CmpOp::Eq => row_val == value,
                CmpOp::NotEq => row_val != value,
                CmpOp::Lt => row_val.partial_cmp(value) == Some(std::cmp::Ordering::Less),
                CmpOp::LtEq => matches!(
                    row_val.partial_cmp(value),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
                CmpOp::Gt => row_val.partial_cmp(value) == Some(std::cmp::Ordering::Greater),
                CmpOp::GtEq => matches!(
                    row_val.partial_cmp(value),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
            }
        }
        FilterExpr::JsonExtract { column, path, op, value } => {
            let idx = match schema.column_index(column) {
                Some(i) => i,
                None => return false,
            };
            let extracted = match &row[idx] {
                Value::Text(s) => json_extract(s, path).unwrap_or(Value::Null),
                _ => Value::Null,
            };
            match op {
                CmpOp::Eq => extracted == *value,
                CmpOp::NotEq => extracted != *value,
                CmpOp::Lt => extracted.partial_cmp(value) == Some(std::cmp::Ordering::Less),
                CmpOp::LtEq => matches!(
                    extracted.partial_cmp(value),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
                CmpOp::Gt => extracted.partial_cmp(value) == Some(std::cmp::Ordering::Greater),
                CmpOp::GtEq => matches!(
                    extracted.partial_cmp(value),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
            }
        }
        FilterExpr::And(a, b) => evaluate_filter(a, row, schema) && evaluate_filter(b, row, schema),
        FilterExpr::Or(a, b) => evaluate_filter(a, row, schema) || evaluate_filter(b, row, schema),
        FilterExpr::Not(inner) => !evaluate_filter(inner, row, schema),
        FilterExpr::IsNull(column) => {
            let idx = match schema.column_index(column) {
                Some(i) => i,
                None => return false,
            };
            row[idx] == Value::Null
        }
        FilterExpr::IsNotNull(column) => {
            let idx = match schema.column_index(column) {
                Some(i) => i,
                None => return false,
            };
            row[idx] != Value::Null
        }
    }
}
