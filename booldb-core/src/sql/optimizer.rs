use crate::catalog::schema::Catalog;
use crate::sql::planner::{CmpOp, FilterExpr, LogicalPlan, Projection};

/// Optimization hints produced by the optimizer.
#[derive(Debug, Clone)]
pub struct QueryHints {
    /// If set, use this index for the primary filter instead of a full scan.
    pub index_scan: Option<IndexScanHint>,
    /// Columns needed from the scan (for early projection).
    pub needed_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct IndexScanHint {
    pub index_name: String,
    pub column: String,
    pub op: CmpOp,
    pub value: crate::types::Value,
}

impl QueryHints {
    pub fn none() -> Self {
        QueryHints {
            index_scan: None,
            needed_columns: None,
        }
    }
}

/// Analyze a logical plan and produce optimization hints.
pub fn optimize(plan: &LogicalPlan, catalog: &Catalog) -> QueryHints {
    match plan {
        LogicalPlan::Select {
            table_name,
            projection,
            filter,
            ..
        } => {
            let mut hints = QueryHints::none();

            // Check if we can use an index for the filter.
            if let Some(filter) = filter {
                if let Some(hint) = try_index_scan(table_name, filter, catalog) {
                    hints.index_scan = Some(hint);
                }
            }

            // Compute needed columns for early projection.
            if let Projection::Columns(cols) = projection {
                hints.needed_columns = Some(cols.clone());
            }

            hints
        }
        _ => QueryHints::none(),
    }
}

/// Try to find an index that matches a simple equality/comparison filter.
fn try_index_scan(
    table_name: &str,
    filter: &FilterExpr,
    catalog: &Catalog,
) -> Option<IndexScanHint> {
    // Only optimize simple column comparisons (not AND/OR).
    let (column, op, value) = match filter {
        FilterExpr::Comparison { column, op, value } => (column, *op, value),
        _ => return None,
    };

    // Check if there's an index on this column.
    let table_meta = catalog.get_table(table_name).ok()?;
    for (idx_name, idx_meta) in &table_meta.indexes {
        let schema_col = table_meta.schema.columns.get(idx_meta.column_index)?;
        if schema_col.name == *column {
            return Some(IndexScanHint {
                index_name: idx_name.clone(),
                column: column.clone(),
                op,
                value: value.clone(),
            });
        }
    }

    None
}

/// Format a logical plan as an EXPLAIN string.
pub fn explain(plan: &LogicalPlan, catalog: &Catalog) -> String {
    let hints = optimize(plan, catalog);
    let mut lines = Vec::new();

    match plan {
        LogicalPlan::Select {
            table_name,
            projection,
            filter,
            joins,
        } => {
            if let Some(ref idx_hint) = hints.index_scan {
                lines.push(format!(
                    "IndexScan: {} using {} ({} {:?} {:?})",
                    table_name, idx_hint.index_name, idx_hint.column, idx_hint.op, idx_hint.value
                ));
            } else {
                lines.push(format!("SeqScan: {}", table_name));
            }

            if let Some(filter) = filter {
                lines.push(format!("  Filter: {}", format_filter(filter)));
            }

            for join in joins {
                lines.push(format!(
                    "  {:?}Join: {} ON {} = {}",
                    join.join_type, join.table_name, join.left_col, join.right_col
                ));
            }

            match projection {
                Projection::All => lines.push("  Projection: *".to_string()),
                Projection::Columns(cols) => {
                    lines.push(format!("  Projection: {}", cols.join(", ")));
                }
            }
        }
        LogicalPlan::Insert { table_name, rows, .. } => {
            lines.push(format!("Insert: {} ({} row(s))", table_name, rows.len()));
        }
        LogicalPlan::Update {
            table_name,
            assignments,
            filter,
        } => {
            let cols: Vec<_> = assignments.iter().map(|(c, _)| c.as_str()).collect();
            lines.push(format!("Update: {} SET {}", table_name, cols.join(", ")));
            if let Some(f) = filter {
                lines.push(format!("  Filter: {}", format_filter(f)));
            }
        }
        LogicalPlan::Delete {
            table_name, filter, ..
        } => {
            lines.push(format!("Delete: {}", table_name));
            if let Some(f) = filter {
                lines.push(format!("  Filter: {}", format_filter(f)));
            }
        }
        LogicalPlan::CreateTable { schema } => {
            lines.push(format!("CreateTable: {}", schema.table_name));
        }
        LogicalPlan::DropTable { table_name } => {
            lines.push(format!("DropTable: {}", table_name));
        }
    }

    lines.join("\n")
}

fn format_filter(filter: &FilterExpr) -> String {
    match filter {
        FilterExpr::Comparison { column, op, value } => {
            let op_str = match op {
                CmpOp::Eq => "=",
                CmpOp::NotEq => "!=",
                CmpOp::Lt => "<",
                CmpOp::LtEq => "<=",
                CmpOp::Gt => ">",
                CmpOp::GtEq => ">=",
            };
            format!("{} {} {}", column, op_str, value)
        }
        FilterExpr::And(a, b) => {
            format!("({} AND {})", format_filter(a), format_filter(b))
        }
        FilterExpr::Or(a, b) => {
            format!("({} OR {})", format_filter(a), format_filter(b))
        }
        FilterExpr::Not(inner) => format!("NOT ({})", format_filter(inner)),
        FilterExpr::IsNull(col) => format!("{} IS NULL", col),
        FilterExpr::IsNotNull(col) => format!("{} IS NOT NULL", col),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::IndexMeta;
    use crate::types::{Column, DataType, Schema, Value};

    fn setup_catalog() -> Catalog {
        let mut cat = Catalog::new();
        cat.create_table(Schema {
            table_name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    primary_key: false,
                },
            ],
        })
        .unwrap();
        cat.add_index(
            "users",
            IndexMeta {
                name: "idx_users_id".to_string(),
                table_name: "users".to_string(),
                column_index: 0,
                root_page_id: 0,
            },
        )
        .unwrap();
        cat
    }

    #[test]
    fn test_index_scan_hint() {
        let cat = setup_catalog();
        let plan = LogicalPlan::Select {
            table_name: "users".to_string(),
            projection: Projection::All,
            filter: Some(FilterExpr::Comparison {
                column: "id".to_string(),
                op: CmpOp::Eq,
                value: Value::Integer(1),
            }),
            joins: vec![],
        };

        let hints = optimize(&plan, &cat);
        assert!(hints.index_scan.is_some());
        assert_eq!(hints.index_scan.unwrap().index_name, "idx_users_id");
    }

    #[test]
    fn test_no_index_for_unindexed_column() {
        let cat = setup_catalog();
        let plan = LogicalPlan::Select {
            table_name: "users".to_string(),
            projection: Projection::All,
            filter: Some(FilterExpr::Comparison {
                column: "name".to_string(),
                op: CmpOp::Eq,
                value: Value::Text("Alice".to_string()),
            }),
            joins: vec![],
        };

        let hints = optimize(&plan, &cat);
        assert!(hints.index_scan.is_none());
    }

    #[test]
    fn test_explain_output() {
        let cat = setup_catalog();
        let plan = LogicalPlan::Select {
            table_name: "users".to_string(),
            projection: Projection::Columns(vec!["id".to_string(), "name".to_string()]),
            filter: Some(FilterExpr::Comparison {
                column: "id".to_string(),
                op: CmpOp::Eq,
                value: Value::Integer(1),
            }),
            joins: vec![],
        };

        let output = explain(&plan, &cat);
        assert!(output.contains("IndexScan"));
        assert!(output.contains("idx_users_id"));
        assert!(output.contains("Projection: id, name"));
    }
}
