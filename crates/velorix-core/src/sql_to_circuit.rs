//! SQL text to `Circuit` IR translation.
//!
//! Parses a SQL SELECT statement using `sqlparser` and produces a `Circuit`
//! that can be incrementalized via the incrementalization algorithm.

use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, JoinConstraint, JoinOperator,
    Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    Value as SqlValue, WindowType,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

use crate::circuit::{
    Circuit, CircuitAggFunc, CircuitColumnRef, CircuitNode,
    CircuitPredicate, CircuitPredicateOp, CircuitProjection, Edge, JoinType, NodeId,
};

/// Simple table schema for SQL → Circuit translation.
#[derive(Clone, Debug)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Error)]
pub enum SqlToCircuitError {
    #[error("SQL parse error: {0}")]
    Parse(#[from] sqlparser::parser::ParserError),
    #[error("unsupported SQL shape: {reason}")]
    Unsupported { reason: String },
    #[error("unknown table: {name}")]
    UnknownTable { name: String },
}

/// Convert a SQL SELECT statement into a `Circuit`.
pub fn sql_to_circuit(
    sql: &str,
    tables: &[TableSchema],
) -> Result<Circuit, SqlToCircuitError> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;
    if statements.len() != 1 {
        return Err(SqlToCircuitError::Unsupported {
            reason: "expected exactly one SELECT statement".into(),
        });
    }
    let statement = statements.pop().expect("validated");
    let query = match statement {
        Statement::Query(q) => q,
        _ => return Err(SqlToCircuitError::Unsupported {
            reason: "expected a SELECT statement".into(),
        }),
    };

    let table_map: HashMap<String, &TableSchema> = tables
        .iter()
        .map(|t| (t.name.to_lowercase(), t))
        .collect();

    let mut ctx = Builder::new(&table_map);
    let output = translate_query(&query, &mut ctx)?;
    Ok(ctx.build(output))
}

struct Builder<'a> {
    table_map: &'a HashMap<String, &'a TableSchema>,
    nodes: Vec<CircuitNode>,
    edges: Vec<Edge>,
    source_map: HashMap<String, NodeId>,
    next_id: NodeId,
}

impl<'a> Builder<'a> {
    fn new(table_map: &'a HashMap<String, &'a TableSchema>) -> Self {
        Self {
            table_map,
            nodes: Vec::new(),
            edges: Vec::new(),
            source_map: HashMap::new(),
            next_id: 0,
        }
    }

    fn alloc(&mut self, node: CircuitNode) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;
        self.nodes.push(node);
        id
    }

    fn connect(&mut self, from: NodeId, to: NodeId) {
        self.edges.push(Edge { from, from_port: 0, to, to_port: 0 });
    }

    fn build(self, output: NodeId) -> Circuit {
        let inputs: Vec<NodeId> = self.source_map.values().copied().collect();
        Circuit {
            nodes: self.nodes,
            edges: self.edges,
            input_node_ids: inputs,
            output_node_id: output,
        }
    }

    fn get_source(&mut self, table_name: &str) -> Result<NodeId, SqlToCircuitError> {
        let key = table_name.to_lowercase();
        if let Some(&id) = self.source_map.get(&key) {
            return Ok(id);
        }
        if !self.table_map.contains_key(&key) {
            return Err(SqlToCircuitError::UnknownTable { name: table_name.into() });
        }
        let id = self.alloc(CircuitNode::Source {
            node_id: 0,
            relation_id: table_name.into(),
        });
        self.source_map.insert(key, id);
        Ok(id)
    }
}

fn translate_query(query: &Query, ctx: &mut Builder) -> Result<NodeId, SqlToCircuitError> {
    let mut current = match *query.body {
        SetExpr::Select(ref select) => translate_select(select, ctx),
        _ => Err(SqlToCircuitError::Unsupported {
            reason: "only SELECT statements are supported".into(),
        }),
    }?;

    // ORDER BY + LIMIT → TopK node
    let has_order = query.order_by.as_ref().map_or(false, |ob| matches!(ob.kind, sqlparser::ast::OrderByKind::Expressions(ref e) if !e.is_empty()));
    let has_limit = query.limit_clause.is_some() || query.fetch.is_some();

    if has_order || has_limit {
    let limit = if let Some(ref lc) = query.limit_clause {
        match lc {
            sqlparser::ast::LimitClause::LimitOffset { limit: Some(ref l), .. } => {
                if let Expr::Value(sqlparser::ast::ValueWithSpan { value: SqlValue::Number(n, _), .. }) = l {
                    n.parse::<usize>().unwrap_or(100)
                } else { 100 }
            }
            _ => 100,
        }
    } else if let Some(ref fetch) = query.fetch {
        if let Some(ref q) = fetch.quantity {
            if let Expr::Value(sqlparser::ast::ValueWithSpan { value: SqlValue::Number(n, _), .. }) = q {
                n.parse::<usize>().unwrap_or(100)
            } else { 100 }
        } else { 100 }
    } else { 100 };

    let offset = if let Some(ref lc) = query.limit_clause {
        match lc {
            sqlparser::ast::LimitClause::LimitOffset { offset: Some(ref o), .. } => {
                if let Expr::Value(sqlparser::ast::ValueWithSpan { value: SqlValue::Number(n, _), .. }) = &o.value {
                    n.parse::<usize>().unwrap_or(0)
                } else { 0 }
            }
            _ => 0,
        }
    } else { 0 };

        if let Some(ref order_by) = query.order_by {
            if let sqlparser::ast::OrderByKind::Expressions(ref exprs) = order_by.kind {
                if let Some(first_order) = exprs.first() {
                    let cr = col_ref(&first_order.expr, current)?;
                    let descending = first_order.options.asc == Some(false);
                    let kid = ctx.alloc(CircuitNode::TopK { node_id: 0, order_by: cr, descending, limit, offset });
                    ctx.connect(current, kid);
                    current = kid;
                }
            }
        }
    }

    Ok(current)
}

fn translate_select(select: &Select, ctx: &mut Builder) -> Result<NodeId, SqlToCircuitError> {
    // FROM
    let mut current = if let Some(twj) = select.from.first() {
        translate_from(twj, ctx)?
    } else {
        return Err(SqlToCircuitError::Unsupported {
            reason: "SELECT without FROM is not supported".into(),
        });
    };

    // WHERE → Filter
    if let Some(ref where_expr) = select.selection {
        let preds = extract_preds(where_expr, current)?;
        if !preds.is_empty() {
            let fid = ctx.alloc(CircuitNode::Filter { node_id: 0, predicates: preds });
            ctx.connect(current, fid);
            current = fid;
        }
    }

    // GROUP BY → Aggregate
    let has_group = matches!(&select.group_by, GroupByExpr::Expressions(e, _) if !e.is_empty());
    if has_group {
        let (keys, funcs) = extract_agg(select, current)?;
        let aid = ctx.alloc(CircuitNode::Aggregate { node_id: 0, group_keys: keys, functions: funcs });
        ctx.connect(current, aid);
        current = aid;
    }

    // HAVING → Filter after aggregate
    if let Some(ref having) = select.having {
        let preds = extract_preds(having, current)?;
        if !preds.is_empty() {
            let fid = ctx.alloc(CircuitNode::Filter { node_id: 0, predicates: preds });
            ctx.connect(current, fid);
            current = fid;
        }
    }

    // Window functions in SELECT projection → RowNumber node
    for item in &select.projection {
        let (func, output_col) = match item {
            SelectItem::UnnamedExpr(Expr::Function(func)) => {
                (func, "row_number".to_string())
            }
            SelectItem::ExprWithAlias { expr: Expr::Function(func), alias } => {
                (func, alias.value.clone())
            }
            _ => continue,
        };
        let name = func.name.to_string().to_uppercase();
        if name == "ROW_NUMBER" {
            if let Some(WindowType::WindowSpec(spec)) = &func.over {
                let partition_keys: Vec<CircuitColumnRef> = spec.partition_by.iter()
                    .filter_map(|e| col_ref(e, current).ok())
                    .collect();
                let order_by = if let Some(first) = spec.order_by.first() {
                    col_ref(&first.expr, current)?
                } else {
                    return Err(SqlToCircuitError::Unsupported {
                        reason: "ROW_NUMBER requires ORDER BY".into(),
                    });
                };
                let descending = spec.order_by.first()
                    .map_or(false, |o| o.options.asc == Some(false));
                let rid = ctx.alloc(CircuitNode::RowNumber {
                    node_id: 0,
                    partition_keys,
                    order_by,
                    descending,
                    output_column_id: output_col,
                });
                ctx.connect(current, rid);
                current = rid;
            }
        }
    }

    // SELECT projection → Project (skip for SELECT * without group by)
    let is_star = select.projection.len() == 1
        && matches!(&select.projection[0], SelectItem::Wildcard(_));
    if !is_star && !has_group {
        let cols = extract_proj(select, current)?;
        let pid = ctx.alloc(CircuitNode::Project { node_id: 0, columns: cols });
        ctx.connect(current, pid);
        current = pid;
    }

    Ok(current)
}

fn translate_from(twj: &TableWithJoins, ctx: &mut Builder) -> Result<NodeId, SqlToCircuitError> {
    let mut current = get_table(&twj.relation, ctx)?;
    for join in &twj.joins {
        let right = get_table(&join.relation, ctx)?;
        let jt = match join.join_operator {
            JoinOperator::Inner(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) | JoinOperator::Left(_) => JoinType::Left,
            JoinOperator::RightOuter(_) | JoinOperator::Right(_) => JoinType::Right,
            _ => JoinType::Inner,
        };
        let (lk, rk) = match &join.join_operator {
            JoinOperator::Inner(c) | JoinOperator::LeftOuter(c) | JoinOperator::Left(c)
            | JoinOperator::RightOuter(c) | JoinOperator::Right(c) => {
                extract_join_keys(c, current, right)?
            }
            _ => (
                CircuitColumnRef { node_id: current, column_id: "__k__".into() },
                CircuitColumnRef { node_id: right, column_id: "__k__".into() },
            ),
        };
        let jid = ctx.alloc(CircuitNode::Join { node_id: 0, join_type: jt, left_key: lk, right_key: rk });
        ctx.edges.push(Edge { from: current, from_port: 0, to: jid, to_port: 0 });
        ctx.edges.push(Edge { from: right, from_port: 1, to: jid, to_port: 1 });
        current = jid;
    }
    Ok(current)
}

fn get_table(factor: &TableFactor, ctx: &mut Builder) -> Result<NodeId, SqlToCircuitError> {
    match factor {
        TableFactor::Table { name, .. } => ctx.get_source(&name.to_string()),
        _ => Err(SqlToCircuitError::Unsupported {
            reason: "subqueries are not supported".into(),
        }),
    }
}

fn extract_preds(expr: &Expr, node: NodeId) -> Result<Vec<CircuitPredicate>, SqlToCircuitError> {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            let mut p = extract_preds(left, node)?;
            p.extend(extract_preds(right, node)?);
            Ok(p)
        }
        Expr::BinaryOp { left, op, right } => {
            let col = col_ref(left, node)?;
            let lit = literal(right)?;
            let op = match op {
                BinaryOperator::Eq => CircuitPredicateOp::Eq,
                BinaryOperator::NotEq => CircuitPredicateOp::Ne,
                BinaryOperator::Lt => CircuitPredicateOp::Lt,
                BinaryOperator::LtEq => CircuitPredicateOp::Le,
                BinaryOperator::Gt => CircuitPredicateOp::Gt,
                BinaryOperator::GtEq => CircuitPredicateOp::Ge,
                _ => return Ok(vec![]),
            };
            Ok(vec![CircuitPredicate { column: col, op, literal: lit }])
        }
        _ => Ok(vec![]),
    }
}

fn col_ref(expr: &Expr, node: NodeId) -> Result<CircuitColumnRef, SqlToCircuitError> {
    match expr {
        Expr::Identifier(i) => Ok(CircuitColumnRef { node_id: node, column_id: i.value.clone() }),
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            Ok(CircuitColumnRef { node_id: node, column_id: idents[1].value.clone() })
        }
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            let col_name = match name.as_str() {
                "COUNT" | "SUM" | "MIN" | "MAX" | "AVG" => {
                    match &func.args {
                        sqlparser::ast::FunctionArguments::List(list) => {
                            list.args.iter().find_map(|a| {
                                if let sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(i))
                                ) = a {
                                    Some(i.value.clone())
                                } else if let sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(
                                        sqlparser::ast::ValueWithSpan {
                                            value: SqlValue::Number(_, _), ..
                                        }
                                    ))
                                ) = a {
                                    Some("1".to_string())
                                } else {
                                    None
                                }
                            }).unwrap_or_else(|| "1".to_string())
                        }
                        _ => "1".to_string(),
                    }
                }
                _ => return Err(SqlToCircuitError::Unsupported {
                    reason: format!("unsupported function in expression: {name}"),
                }),
            };
            Ok(CircuitColumnRef { node_id: node, column_id: col_name })
        }
        _ => Err(SqlToCircuitError::Unsupported { reason: "non-column expression".into() }),
    }
}

fn literal(expr: &Expr) -> Result<serde_json::Value, SqlToCircuitError> {
    match expr {
        Expr::Value(v) => match &v.value {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() { Ok(serde_json::json!(i)) }
                else { Ok(serde_json::Value::String(n.clone())) }
            }
            SqlValue::SingleQuotedString(s) => Ok(serde_json::json!(s)),
            SqlValue::Boolean(b) => Ok(serde_json::json!(b)),
            SqlValue::Null => Ok(serde_json::Value::Null),
            _ => Err(SqlToCircuitError::Unsupported { reason: "unsupported literal".into() }),
        },
        _ => Err(SqlToCircuitError::Unsupported { reason: "non-literal".into() }),
    }
}

fn extract_agg(
    select: &Select, node: NodeId,
) -> Result<(Vec<CircuitColumnRef>, Vec<CircuitAggFunc>), SqlToCircuitError> {
    let keys = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => {
            exprs.iter().map(|e| col_ref(e, node)).collect::<Result<Vec<_>, _>>()?
        }
        _ => vec![],
    };
    let mut funcs = Vec::new();
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(Expr::Function(func)) = item {
            let name = func.name.to_string().to_uppercase();
            // For now, extract the first argument as a column reference if it's an identifier
            let first_arg_col = match &func.args {
                sqlparser::ast::FunctionArguments::List(list) => {
                    list.args.iter().find_map(|a| {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(i))
                        ) = a {
                            Some(CircuitColumnRef { node_id: node, column_id: i.value.clone() })
                        } else {
                            None
                        }
                    })
                }
                _ => None,
            };
            match name.as_str() {
                "SUM" => if let Some(c) = first_arg_col { funcs.push(CircuitAggFunc::Sum(c)); },
                "COUNT" => funcs.push(CircuitAggFunc::Count),
                "MIN" => if let Some(c) = first_arg_col { funcs.push(CircuitAggFunc::Min(c)); },
                "MAX" => if let Some(c) = first_arg_col { funcs.push(CircuitAggFunc::Max(c)); },
                "AVG" => if let Some(c) = first_arg_col { funcs.push(CircuitAggFunc::Avg(c)); },
                _ => {}
            }
        }
    }
    Ok((keys, funcs))
}

fn extract_proj(
    select: &Select, node: NodeId,
) -> Result<Vec<CircuitProjection>, SqlToCircuitError> {
    let mut cols = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Function(func)) if func.over.is_some() => {
                // Window functions are handled by RowNumber node, skip in projection
            }
            SelectItem::UnnamedExpr(e) => {
                let cr = col_ref(e, node)?;
                cols.push(CircuitProjection { source_column: cr.clone(), output_column_id: cr.column_id });
            }
            SelectItem::ExprWithAlias { expr: Expr::Function(func), .. } if func.over.is_some() => {
                // Window functions with alias are handled by RowNumber node
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let cr = col_ref(expr, node)?;
                cols.push(CircuitProjection { source_column: cr, output_column_id: alias.value.clone() });
            }
            _ => {}
        }
    }
    Ok(cols)
}

fn extract_join_keys(
    constraint: &JoinConstraint, left: NodeId, right: NodeId,
) -> Result<(CircuitColumnRef, CircuitColumnRef), SqlToCircuitError> {
    match constraint {
        JoinConstraint::On(Expr::BinaryOp { left: l, op: BinaryOperator::Eq, right: r }) => {
            Ok((col_ref(l, left)?, col_ref(r, right)?))
        }
        JoinConstraint::Using(attrs) if attrs.len() == 1 => {
            let obj_name = &attrs[0];
            // ObjectName wraps Vec<ObjectNamePart>, extract the first identifier
            let name = match &obj_name.0.first() {
                Some(sqlparser::ast::ObjectNamePart::Identifier(i)) => i.value.clone(),
                _ => return Err(SqlToCircuitError::Unsupported {
                    reason: "USING requires simple column names".into(),
                }),
            };
            Ok((
                CircuitColumnRef { node_id: left, column_id: name.clone() },
                CircuitColumnRef { node_id: right, column_id: name },
            ))
        }
        _ => Err(SqlToCircuitError::Unsupported {
            reason: "JOIN requires ON <col> = <col>".into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tbl(name: &str, cols: &[&str]) -> TableSchema {
        TableSchema { name: name.into(), columns: cols.iter().map(|s| s.to_string()).collect() }
    }

    #[test]
    fn simple_select_star() {
        let c = sql_to_circuit("SELECT * FROM users", &[tbl("users", &["id", "name"])]).unwrap();
        assert_eq!(c.nodes.len(), 1);
        assert!(matches!(c.nodes[0], CircuitNode::Source { .. }));
    }

    #[test]
    fn select_with_where() {
        let c = sql_to_circuit(
            "SELECT id FROM users WHERE age >= 18",
            &[tbl("users", &["id", "age"])],
        ).unwrap();
        assert!(c.nodes.len() >= 2);
    }

    #[test]
    fn select_with_group_by() {
        let c = sql_to_circuit(
            "SELECT dept, SUM(sal) FROM emp GROUP BY dept",
            &[tbl("emp", &["dept", "sal"])],
        ).unwrap();
        assert!(c.nodes.len() >= 2);
    }

    #[test]
    fn select_with_having() {
        let c = sql_to_circuit(
            "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5",
            &[tbl("emp", &["dept", "sal"])],
        ).unwrap();
        // Source → Aggregate → Filter (having)
        assert!(c.nodes.len() >= 3);
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::Filter { .. })));
    }

    #[test]
    fn select_with_order_by_limit() {
        let c = sql_to_circuit(
            "SELECT name FROM users ORDER BY name LIMIT 10",
            &[tbl("users", &["id", "name"])],
        ).unwrap();
        // Source → TopK
        assert!(c.nodes.len() >= 2);
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::TopK { limit: 10, .. })));
    }

    #[test]
    fn select_with_having_and_order_by() {
        let c = sql_to_circuit(
            "SELECT dept, SUM(sal) as total FROM emp GROUP BY dept HAVING SUM(sal) > 1000 ORDER BY total DESC LIMIT 5",
            &[tbl("emp", &["dept", "sal"])],
        ).unwrap();
        // Source → Aggregate → Filter → TopK
        assert!(c.nodes.len() >= 4);
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::Aggregate { .. })));
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::Filter { .. })));
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::TopK { limit: 5, .. })));
    }

    #[test]
    fn select_order_by_aggregate_function() {
        let c = sql_to_circuit(
            "SELECT dept, SUM(sal) FROM emp GROUP BY dept ORDER BY SUM(sal) DESC",
            &[tbl("emp", &["dept", "sal"])],
        ).unwrap();
        // Source → Aggregate → TopK
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::Aggregate { .. })));
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::TopK { descending: true, .. })));
    }

    #[test]
    fn select_order_by_count_star() {
        let c = sql_to_circuit(
            "SELECT dept, COUNT(*) FROM emp GROUP BY dept ORDER BY COUNT(*) DESC LIMIT 3",
            &[tbl("emp", &["dept", "sal"])],
        ).unwrap();
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::TopK { limit: 3, descending: true, .. })));
    }

    #[test]
    fn select_row_number_window_function() {
        let c = sql_to_circuit(
            "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY name) AS rn FROM emp",
            &[tbl("emp", &["name", "dept", "sal"])],
        ).unwrap();
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::RowNumber { .. })));
    }

    #[test]
    fn select_row_number_no_partition() {
        let c = sql_to_circuit(
            "SELECT name, ROW_NUMBER() OVER (ORDER BY name DESC) AS rn FROM emp",
            &[tbl("emp", &["name"])],
        ).unwrap();
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::RowNumber { descending: true, .. })));
    }

    #[test]
    fn select_having_order_by_limit_combined() {
        let c = sql_to_circuit(
            "SELECT dept, SUM(sal) as total, COUNT(*) as cnt FROM emp GROUP BY dept HAVING COUNT(*) > 3 ORDER BY total DESC, cnt ASC LIMIT 10",
            &[tbl("emp", &["dept", "sal"])],
        ).unwrap();
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::Aggregate { .. })));
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::Filter { .. })));
        assert!(c.nodes.iter().any(|n| matches!(n, CircuitNode::TopK { limit: 10, .. })));
    }
}
