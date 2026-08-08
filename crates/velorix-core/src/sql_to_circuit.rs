//! SQL text to `Circuit` IR translation.
//!
//! Parses a SQL SELECT statement using `sqlparser` and produces a `Circuit`
//! that can be incrementalized via the incrementalization algorithm.

use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, JoinConstraint, JoinOperator, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue, WindowType,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

use crate::circuit::{
    Circuit, CircuitAggFunc, CircuitColumnRef, CircuitDataType, CircuitExpr, CircuitFilterExpr,
    CircuitNode, CircuitPredicate, CircuitPredicateOp, CircuitProjection, Edge, JoinType, NodeId,
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
pub fn sql_to_circuit(sql: &str, tables: &[TableSchema]) -> Result<Circuit, SqlToCircuitError> {
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
        _ => {
            return Err(SqlToCircuitError::Unsupported {
                reason: "expected a SELECT statement".into(),
            })
        }
    };

    let table_map: HashMap<String, &TableSchema> =
        tables.iter().map(|t| (t.name.to_lowercase(), t)).collect();

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
        self.edges.push(Edge {
            from,
            from_port: 0,
            to,
            to_port: 0,
        });
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
            return Err(SqlToCircuitError::UnknownTable {
                name: table_name.into(),
            });
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
    // Process CTEs (WITH clause) if present
    if let Some(ref with) = query.with {
        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.to_lowercase();
            let cte_query = &cte.query;
            let cte_node = translate_query(cte_query, ctx)?;
            ctx.source_map.insert(cte_name, cte_node);
        }
    }

    let mut current = match *query.body {
        SetExpr::Select(ref select) => translate_select(select, ctx),
        _ => Err(SqlToCircuitError::Unsupported {
            reason: "only SELECT statements are supported".into(),
        }),
    }?;

    // ORDER BY + LIMIT → TopK node
    let has_order = query.order_by.as_ref().map_or(
        false,
        |ob| matches!(ob.kind, sqlparser::ast::OrderByKind::Expressions(ref e) if !e.is_empty()),
    );
    let has_limit = query.limit_clause.is_some() || query.fetch.is_some();

    if has_order || has_limit {
        let limit = if let Some(ref lc) = query.limit_clause {
            match lc {
                sqlparser::ast::LimitClause::LimitOffset {
                    limit: Some(ref l), ..
                } => {
                    if let Expr::Value(sqlparser::ast::ValueWithSpan {
                        value: SqlValue::Number(n, _),
                        ..
                    }) = l
                    {
                        n.parse::<usize>().unwrap_or(100)
                    } else {
                        100
                    }
                }
                _ => 100,
            }
        } else if let Some(ref fetch) = query.fetch {
            if let Some(ref q) = fetch.quantity {
                if let Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: SqlValue::Number(n, _),
                    ..
                }) = q
                {
                    n.parse::<usize>().unwrap_or(100)
                } else {
                    100
                }
            } else {
                100
            }
        } else {
            100
        };

        let offset = if let Some(ref lc) = query.limit_clause {
            match lc {
                sqlparser::ast::LimitClause::LimitOffset {
                    offset: Some(ref o),
                    ..
                } => {
                    if let Expr::Value(sqlparser::ast::ValueWithSpan {
                        value: SqlValue::Number(n, _),
                        ..
                    }) = &o.value
                    {
                        n.parse::<usize>().unwrap_or(0)
                    } else {
                        0
                    }
                }
                _ => 0,
            }
        } else {
            0
        };

        if let Some(ref order_by) = query.order_by {
            if let sqlparser::ast::OrderByKind::Expressions(ref exprs) = order_by.kind {
                if let Some(first_order) = exprs.first() {
                    let cr = col_ref(&first_order.expr, current)?;
                    let descending = first_order.options.asc == Some(false);
                    let kid = ctx.alloc(CircuitNode::TopK {
                        node_id: 0,
                        order_by: cr,
                        descending,
                        limit,
                        offset,
                    });
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
        let pred_expr = extract_filter_expr(where_expr, current)?;
        if !matches!(pred_expr, CircuitFilterExpr::AlwaysTrue) {
            let fid = ctx.alloc(CircuitNode::Filter {
                node_id: 0,
                predicate: pred_expr,
            });
            ctx.connect(current, fid);
            current = fid;
        }
    }

    // GROUP BY → Aggregate
    let has_latest_by_key = select.projection.iter().any(|item| {
        let function = match item {
            SelectItem::UnnamedExpr(Expr::Function(function))
            | SelectItem::ExprWithAlias {
                expr: Expr::Function(function),
                ..
            } => function,
            _ => return false,
        };
        matches!(
            function.name.to_string().to_uppercase().as_str(),
            "ARG_MAX" | "ARG_MIN"
        )
    });
    let has_group = !has_latest_by_key
        && matches!(&select.group_by, GroupByExpr::Expressions(e, _) if !e.is_empty());
    if has_group {
        let (keys, funcs, output_aliases, pre_computed) = extract_agg(select, current)?;
        for (col_name, expr) in pre_computed {
            let mid = ctx.alloc(CircuitNode::Map {
                node_id: 0,
                expr,
                output_column_id: col_name.clone(),
            });
            ctx.connect(current, mid);
            current = mid;
        }
        let aid = ctx.alloc(CircuitNode::Aggregate {
            node_id: 0,
            group_keys: keys,
            functions: funcs,
            output_aliases,
        });
        ctx.connect(current, aid);
        current = aid;
    }

    // HAVING → Filter after aggregate
    if let Some(ref having) = select.having {
        let pred_expr = extract_filter_expr(having, current)?;
        if !matches!(pred_expr, CircuitFilterExpr::AlwaysTrue) {
            let fid = ctx.alloc(CircuitNode::Filter {
                node_id: 0,
                predicate: pred_expr,
            });
            ctx.connect(current, fid);
            current = fid;
        }
    }

    // Window functions in SELECT projection → RowNumber or LatestByKey node
    for item in &select.projection {
        let (func, output_col) = match item {
            SelectItem::UnnamedExpr(Expr::Function(func)) => (func, "row_number".to_string()),
            SelectItem::ExprWithAlias {
                expr: Expr::Function(func),
                alias,
            } => (func, alias.value.clone()),
            _ => continue,
        };
        let name = func.name.to_string().to_uppercase();
        if name == "ROW_NUMBER" {
            if let Some(WindowType::WindowSpec(spec)) = &func.over {
                let partition_keys: Vec<CircuitColumnRef> = spec
                    .partition_by
                    .iter()
                    .filter_map(|e| col_ref(e, current).ok())
                    .collect();
                let order_by = if let Some(first) = spec.order_by.first() {
                    col_ref(&first.expr, current)?
                } else {
                    return Err(SqlToCircuitError::Unsupported {
                        reason: "ROW_NUMBER requires ORDER BY".into(),
                    });
                };
                let descending = spec
                    .order_by
                    .first()
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
        } else if name == "ARG_MAX" || name == "ARG_MIN" {
            // ARG_MAX(value_col, order_col) or ARG_MIN(value_col, order_col)
            if let sqlparser::ast::FunctionArguments::List(list) = &func.args {
                if list.args.len() == 2 {
                    let value_arg = &list.args[0];
                    let order_arg = &list.args[1];
                    if let (
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(value_expr),
                        ),
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(order_expr),
                        ),
                    ) = (value_arg, order_arg)
                    {
                        let value_col = col_ref(value_expr, current)?;
                        let order_col = col_ref(order_expr, current)?;
                        let descending = name == "ARG_MAX";
                        let lid = ctx.alloc(CircuitNode::LatestByKey {
                            node_id: 0,
                            key: value_col,
                            order_by: order_col,
                            descending,
                        });
                        ctx.connect(current, lid);
                        current = lid;
                    }
                }
            }
        }
    }

    // SELECT projection → Project (skip for SELECT * without group by)
    let is_star =
        select.projection.len() == 1 && matches!(&select.projection[0], SelectItem::Wildcard(_));
    if !is_star && !has_group {
        let (cols, computed) = extract_proj(select, current)?;
        // Insert Map nodes for computed expressions
        for (col_name, expr) in computed {
            let mid = ctx.alloc(CircuitNode::Map {
                node_id: 0,
                expr,
                output_column_id: col_name,
            });
            ctx.connect(current, mid);
            current = mid;
        }
        if !cols.is_empty() {
            let pid = ctx.alloc(CircuitNode::Project {
                node_id: 0,
                columns: cols,
            });
            ctx.connect(current, pid);
            current = pid;
        }
    }

    // SELECT DISTINCT applies to projected rows, not source rows.
    if select.distinct.is_some() {
        let did = ctx.alloc(CircuitNode::Distinct { node_id: 0 });
        ctx.connect(current, did);
        current = did;
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
            JoinOperator::Inner(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::Left(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::Right(c) => extract_join_keys(c, current, right)?,
            _ => (
                CircuitColumnRef {
                    node_id: current,
                    column_id: "__k__".into(),
                },
                CircuitColumnRef {
                    node_id: right,
                    column_id: "__k__".into(),
                },
            ),
        };
        let jid = ctx.alloc(CircuitNode::Join {
            node_id: 0,
            join_type: jt,
            left_key: lk,
            right_key: rk,
        });
        ctx.edges.push(Edge {
            from: current,
            from_port: 0,
            to: jid,
            to_port: 0,
        });
        ctx.edges.push(Edge {
            from: right,
            from_port: 1,
            to: jid,
            to_port: 1,
        });
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

fn extract_filter_expr(expr: &Expr, node: NodeId) -> Result<CircuitFilterExpr, SqlToCircuitError> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let l = extract_filter_expr(left, node)?;
            let r = extract_filter_expr(right, node)?;
            Ok(CircuitFilterExpr::And(vec![l, r]))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            let l = extract_filter_expr(left, node)?;
            let r = extract_filter_expr(right, node)?;
            Ok(CircuitFilterExpr::Or(vec![l, r]))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if matches!(right.as_ref(), Expr::Value(v) if matches!(v.value, SqlValue::Null)) {
                Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                    column: col_ref(left, node)?,
                    op: CircuitPredicateOp::IsNull,
                    literal: serde_json::Value::Null,
                    literals: vec![],
                }))
            } else if matches!(left.as_ref(), Expr::Value(v) if matches!(v.value, SqlValue::Null)) {
                Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                    column: col_ref(right, node)?,
                    op: CircuitPredicateOp::IsNull,
                    literal: serde_json::Value::Null,
                    literals: vec![],
                }))
            } else if literal(right).is_ok() {
                let col = col_ref_or_aggref(left, node)?;
                let lit = literal_or_null(right)?;
                Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                    column: col,
                    op: CircuitPredicateOp::Eq,
                    literal: lit,
                    literals: vec![],
                }))
            } else {
                comparison_expr(left, CircuitPredicateOp::Eq, right, node)
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::NotEq,
            right,
        } => {
            if matches!(right.as_ref(), Expr::Value(v) if matches!(v.value, SqlValue::Null)) {
                Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                    column: col_ref(left, node)?,
                    op: CircuitPredicateOp::IsNotNull,
                    literal: serde_json::Value::Null,
                    literals: vec![],
                }))
            } else if matches!(left.as_ref(), Expr::Value(v) if matches!(v.value, SqlValue::Null)) {
                Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                    column: col_ref(right, node)?,
                    op: CircuitPredicateOp::IsNotNull,
                    literal: serde_json::Value::Null,
                    literals: vec![],
                }))
            } else if literal(right).is_ok() {
                let col = col_ref_or_aggref(left, node)?;
                let lit = literal_or_null(right)?;
                Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                    column: col,
                    op: CircuitPredicateOp::Ne,
                    literal: lit,
                    literals: vec![],
                }))
            } else {
                comparison_expr(left, CircuitPredicateOp::Ne, right, node)
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Lt,
            right,
        } => {
            if literal(right).is_err() {
                return comparison_expr(left, CircuitPredicateOp::Lt, right, node);
            }
            let col = col_ref_or_aggref(left, node)?;
            let lit = literal_or_null(right)?;
            Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                column: col,
                op: CircuitPredicateOp::Lt,
                literal: lit,
                literals: vec![],
            }))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::LtEq,
            right,
        } => {
            if literal(right).is_err() {
                return comparison_expr(left, CircuitPredicateOp::Le, right, node);
            }
            let col = col_ref_or_aggref(left, node)?;
            let lit = literal_or_null(right)?;
            Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                column: col,
                op: CircuitPredicateOp::Le,
                literal: lit,
                literals: vec![],
            }))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Gt,
            right,
        } => {
            if literal(right).is_err() {
                return comparison_expr(left, CircuitPredicateOp::Gt, right, node);
            }
            let col = col_ref_or_aggref(left, node)?;
            let lit = literal_or_null(right)?;
            Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                column: col,
                op: CircuitPredicateOp::Gt,
                literal: lit,
                literals: vec![],
            }))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::GtEq,
            right,
        } => {
            if literal(right).is_err() {
                return comparison_expr(left, CircuitPredicateOp::Ge, right, node);
            }
            let col = col_ref_or_aggref(left, node)?;
            let lit = literal_or_null(right)?;
            Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                column: col,
                op: CircuitPredicateOp::Ge,
                literal: lit,
                literals: vec![],
            }))
        }
        Expr::BinaryOp {
            left: _,
            op: BinaryOperator::Plus,
            right: _,
        }
        | Expr::BinaryOp {
            left: _,
            op: BinaryOperator::Minus,
            right: _,
        }
        | Expr::BinaryOp {
            left: _,
            op: BinaryOperator::Multiply,
            right: _,
        }
        | Expr::BinaryOp {
            left: _,
            op: BinaryOperator::Divide,
            right: _,
        } => Err(SqlToCircuitError::Unsupported {
            reason: "standalone arithmetic expressions are not boolean predicates".into(),
        }),
        Expr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let col = col_ref_or_aggref(expr, node)?;
            let lit = literal_or_null(pattern)?;
            let pred = CircuitFilterExpr::Predicate(CircuitPredicate {
                column: col,
                op: CircuitPredicateOp::Like,
                literal: lit,
                literals: vec![],
            });
            if *negated {
                Ok(CircuitFilterExpr::Not(Box::new(pred)))
            } else {
                Ok(pred)
            }
        }
        Expr::ILike {
            negated,
            expr,
            pattern,
            ..
        } => {
            let col = col_ref_or_aggref(expr, node)?;
            let lit = literal_or_null(pattern)?;
            let pred = CircuitFilterExpr::Predicate(CircuitPredicate {
                column: col,
                op: CircuitPredicateOp::Like,
                literal: lit,
                literals: vec![],
            });
            if *negated {
                Ok(CircuitFilterExpr::Not(Box::new(pred)))
            } else {
                Ok(pred)
            }
        }
        Expr::IsDistinctFrom(left, right) => {
            let col = col_ref_or_aggref(left, node)?;
            let lit = literal_or_null(right)?;
            Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                column: col,
                op: CircuitPredicateOp::IsDistinctFrom,
                literal: lit,
                literals: vec![],
            }))
        }
        Expr::IsNotDistinctFrom(left, right) => {
            let col = col_ref_or_aggref(left, node)?;
            let lit = literal_or_null(right)?;
            Ok(CircuitFilterExpr::Not(Box::new(
                CircuitFilterExpr::Predicate(CircuitPredicate {
                    column: col,
                    op: CircuitPredicateOp::IsDistinctFrom,
                    literal: lit,
                    literals: vec![],
                }),
            )))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col = col_ref_or_aggref(expr, node)?;
            let mut values = Vec::new();
            for item in list {
                values.push(literal_or_null(item)?);
            }
            let first = values.first().cloned().unwrap_or(serde_json::Value::Null);
            let op = if *negated {
                CircuitPredicateOp::NotIn
            } else {
                CircuitPredicateOp::In
            };
            Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                column: col,
                op,
                literal: first,
                literals: values,
            }))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let col = col_ref_or_aggref(expr, node)?;
            let low_val = literal_or_null(low)?;
            let high_val = literal_or_null(high)?;
            let pred = CircuitPredicate {
                column: col,
                op: CircuitPredicateOp::Between,
                literal: serde_json::Value::Null,
                literals: vec![low_val, high_val],
            };
            if *negated {
                Ok(CircuitFilterExpr::Not(Box::new(
                    CircuitFilterExpr::Predicate(pred),
                )))
            } else {
                Ok(CircuitFilterExpr::Predicate(pred))
            }
        }
        Expr::IsNull(inner) => {
            let col = col_ref(inner, node)?;
            Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                column: col,
                op: CircuitPredicateOp::IsNull,
                literal: serde_json::Value::Null,
                literals: vec![],
            }))
        }
        Expr::IsNotNull(inner) => {
            let col = col_ref(inner, node)?;
            Ok(CircuitFilterExpr::Predicate(CircuitPredicate {
                column: col,
                op: CircuitPredicateOp::IsNotNull,
                literal: serde_json::Value::Null,
                literals: vec![],
            }))
        }
        Expr::Nested(inner) => extract_filter_expr(inner, node),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr,
        } => {
            let inner = extract_filter_expr(expr, node)?;
            Ok(CircuitFilterExpr::Not(Box::new(inner)))
        }
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => {
            // Unary minus in filter context: treat as negation of value
            extract_filter_expr(expr, node)
        }
        _ => Err(SqlToCircuitError::Unsupported {
            reason: format!("unsupported filter expression: {expr:?}"),
        }),
    }
}

fn comparison_expr(
    left: &Expr,
    op: CircuitPredicateOp,
    right: &Expr,
    node: NodeId,
) -> Result<CircuitFilterExpr, SqlToCircuitError> {
    Ok(CircuitFilterExpr::Comparison {
        left: expr_to_circuit(left, node)?,
        op,
        right: expr_to_circuit(right, node)?,
    })
}

/// Try to extract a column reference, including aggregate output aliases.
/// For HAVING clauses, `sum(s.score)` should resolve to column "sum".
fn col_ref_or_aggref(expr: &Expr, node: NodeId) -> Result<CircuitColumnRef, SqlToCircuitError> {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            // Map aggregate function names to their output column aliases
            match name.as_str() {
                "COUNT" | "SUM" | "MIN" | "MAX" | "AVG" => Ok(CircuitColumnRef {
                    node_id: node,
                    column_id: name.to_lowercase(),
                }),
                _ => col_ref(expr, node),
            }
        }
        _ => col_ref(expr, node),
    }
}

/// Extract a literal value, or Null for non-literal expressions.
fn literal_or_null(expr: &Expr) -> Result<serde_json::Value, SqlToCircuitError> {
    literal(expr)
        .ok()
        .map(Ok)
        .unwrap_or(Ok(serde_json::Value::Null))
}

/// Convert a SQL expression to a CircuitExpr (for Map nodes and aggregate arguments).
fn expr_to_circuit(expr: &Expr, node: NodeId) -> Result<CircuitExpr, SqlToCircuitError> {
    match expr {
        Expr::Identifier(i) => Ok(CircuitExpr::Column(CircuitColumnRef {
            node_id: node,
            column_id: i.value.clone(),
        })),
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            Ok(CircuitExpr::Column(CircuitColumnRef {
                node_id: node,
                column_id: idents[1].value.clone(),
            }))
        }
        Expr::Value(v) => match &v.value {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(CircuitExpr::Literal(serde_json::json!(i)))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(CircuitExpr::Literal(serde_json::json!(f)))
                } else {
                    Ok(CircuitExpr::Literal(serde_json::Value::String(n.clone())))
                }
            }
            SqlValue::SingleQuotedString(s) => Ok(CircuitExpr::Literal(serde_json::json!(s))),
            SqlValue::Boolean(b) => Ok(CircuitExpr::Literal(serde_json::json!(b))),
            SqlValue::Null => Ok(CircuitExpr::Literal(serde_json::Value::Null)),
            _ => Err(SqlToCircuitError::Unsupported {
                reason: "unsupported literal".into(),
            }),
        },
        Expr::Nested(inner) => expr_to_circuit(inner, node),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Plus,
            right,
        } => Ok(CircuitExpr::Add(
            Box::new(expr_to_circuit(left, node)?),
            Box::new(expr_to_circuit(right, node)?),
        )),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Minus,
            right,
        } => Ok(CircuitExpr::Sub(
            Box::new(expr_to_circuit(left, node)?),
            Box::new(expr_to_circuit(right, node)?),
        )),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Multiply,
            right,
        } => Ok(CircuitExpr::Mul(
            Box::new(expr_to_circuit(left, node)?),
            Box::new(expr_to_circuit(right, node)?),
        )),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Divide,
            right,
        } => Ok(CircuitExpr::Div(
            Box::new(expr_to_circuit(left, node)?),
            Box::new(expr_to_circuit(right, node)?),
        )),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Modulo,
            right,
        } => Ok(CircuitExpr::Modulo(
            Box::new(expr_to_circuit(left, node)?),
            Box::new(expr_to_circuit(right, node)?),
        )),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => Ok(CircuitExpr::Neg(Box::new(expr_to_circuit(expr, node)?))),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Plus,
            expr,
        } => expr_to_circuit(expr, node),
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();

            let raw_args: Vec<Expr> = match &func.args {
                sqlparser::ast::FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .filter_map(|a| {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) = a
                        {
                            Some(e.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
                _ => vec![],
            };

            match name.as_str() {
                "IF" => {
                    if raw_args.len() >= 3 {
                        let cond_sql = &raw_args[0];
                        let then_expr = expr_to_circuit(&raw_args[1], node)?;
                        let else_expr = expr_to_circuit(&raw_args[2], node)?;
                        let filter = extract_filter_expr(cond_sql, node)?;
                        Ok(CircuitExpr::If {
                            condition: Box::new(filter),
                            then_expr: Box::new(then_expr),
                            else_expr: Box::new(else_expr),
                        })
                    } else {
                        Err(SqlToCircuitError::Unsupported {
                            reason: "IF requires 3 arguments".into(),
                        })
                    }
                }
                _ => {
                    let args = raw_args
                        .iter()
                        .map(|e| expr_to_circuit(e, node))
                        .collect::<Result<Vec<_>, _>>()?;
                    match name.as_str() {
                        "ABS" => {
                            if let Some(arg) = args.into_iter().next() {
                                Ok(CircuitExpr::Abs(Box::new(arg)))
                            } else {
                                Err(SqlToCircuitError::Unsupported {
                                    reason: "ABS requires 1 argument".into(),
                                })
                            }
                        }
                        "GREATEST" => Ok(CircuitExpr::Greatest(args)),
                        "LEAST" => Ok(CircuitExpr::Least(args)),
                        "COALESCE" => Ok(args
                            .into_iter()
                            .next()
                            .unwrap_or(CircuitExpr::Literal(serde_json::Value::Null))),
                        "COUNT" | "SUM" | "MIN" | "MAX" | "AVG" | "ARG_MAX" | "ARG_MIN" => Ok(args
                            .into_iter()
                            .next()
                            .unwrap_or(CircuitExpr::Literal(serde_json::Value::Null))),
                        _ => Err(SqlToCircuitError::Unsupported {
                            reason: format!("unsupported function in expression: {name}"),
                        }),
                    }
                }
            }
        }
        Expr::Cast {
            expr, data_type, ..
        } => {
            let inner = expr_to_circuit(expr, node)?;
            let to_type = match data_type {
                sqlparser::ast::DataType::Int64 | sqlparser::ast::DataType::Int(_) => {
                    CircuitDataType::I64
                }
                sqlparser::ast::DataType::Float64 | sqlparser::ast::DataType::Float(_) => {
                    CircuitDataType::F64
                }
                sqlparser::ast::DataType::Boolean | sqlparser::ast::DataType::Bool => {
                    CircuitDataType::Bool
                }
                sqlparser::ast::DataType::Varchar(_)
                | sqlparser::ast::DataType::Text
                | sqlparser::ast::DataType::String(_) => CircuitDataType::String,
                _ => CircuitDataType::I64,
            };
            Ok(CircuitExpr::Cast {
                expr: Box::new(inner),
                to_type,
            })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let mut when_clauses = Vec::new();
            for clause in conditions {
                let cond = if let Some(ref case_operand) = operand {
                    let eq_expr = Expr::BinaryOp {
                        left: Box::new(*case_operand.clone()),
                        op: BinaryOperator::Eq,
                        right: Box::new(clause.condition.clone()),
                    };
                    extract_filter_expr(&eq_expr, node)?
                } else {
                    extract_filter_expr(&clause.condition, node)?
                };
                let result = expr_to_circuit(&clause.result, node)?;
                when_clauses.push((cond, result));
            }
            let else_res = else_result
                .as_ref()
                .map(|e| expr_to_circuit(e, node).map(Box::new))
                .transpose()?;
            Ok(CircuitExpr::CaseWhen {
                when_clauses,
                else_result: else_res,
            })
        }
        Expr::Substring { .. } => {
            // Simplified: just return the string as-is
            Err(SqlToCircuitError::Unsupported {
                reason: "SUBSTRING not supported in expressions".into(),
            })
        }
        _ => Err(SqlToCircuitError::Unsupported {
            reason: format!("unsupported expression: {:?}", expr),
        }),
    }
}

fn col_ref(expr: &Expr, node: NodeId) -> Result<CircuitColumnRef, SqlToCircuitError> {
    match expr {
        Expr::Identifier(i) => Ok(CircuitColumnRef {
            node_id: node,
            column_id: i.value.clone(),
        }),
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(CircuitColumnRef {
            node_id: node,
            column_id: idents[1].value.clone(),
        }),
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            let col_name = match name.as_str() {
                "COUNT" | "SUM" | "MIN" | "MAX" | "AVG" => match &func.args {
                    sqlparser::ast::FunctionArguments::List(list) => list
                        .args
                        .iter()
                        .find_map(|a| {
                            if let sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(i)),
                            ) = a
                            {
                                Some(i.value.clone())
                            } else if let sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(
                                    sqlparser::ast::ValueWithSpan {
                                        value: SqlValue::Number(_, _),
                                        ..
                                    },
                                )),
                            ) = a
                            {
                                Some("1".to_string())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_else(|| "1".to_string()),
                    _ => "1".to_string(),
                },
                _ => {
                    return Err(SqlToCircuitError::Unsupported {
                        reason: format!("unsupported function in expression: {name}"),
                    })
                }
            };
            Ok(CircuitColumnRef {
                node_id: node,
                column_id: col_name,
            })
        }
        _ => Err(SqlToCircuitError::Unsupported {
            reason: "non-column expression".into(),
        }),
    }
}

fn literal(expr: &Expr) -> Result<serde_json::Value, SqlToCircuitError> {
    match expr {
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => match literal(expr)? {
            serde_json::Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    value
                        .checked_neg()
                        .map(|value| serde_json::json!(value))
                        .ok_or_else(|| SqlToCircuitError::Unsupported {
                            reason: "numeric literal overflow".into(),
                        })
                } else if let Some(value) = number.as_f64() {
                    serde_json::Number::from_f64(-value)
                        .map(serde_json::Value::Number)
                        .ok_or_else(|| SqlToCircuitError::Unsupported {
                            reason: "unsupported numeric literal".into(),
                        })
                } else {
                    Err(SqlToCircuitError::Unsupported {
                        reason: "unsupported numeric literal".into(),
                    })
                }
            }
            _ => Err(SqlToCircuitError::Unsupported {
                reason: "unary minus requires a numeric literal".into(),
            }),
        },
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Plus,
            expr,
        } => literal(expr),
        Expr::Value(v) => match &v.value {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(serde_json::json!(i))
                } else {
                    Ok(serde_json::Value::String(n.clone()))
                }
            }
            SqlValue::SingleQuotedString(s) => Ok(serde_json::json!(s)),
            SqlValue::Boolean(b) => Ok(serde_json::json!(b)),
            SqlValue::Null => Ok(serde_json::Value::Null),
            _ => Err(SqlToCircuitError::Unsupported {
                reason: "unsupported literal".into(),
            }),
        },
        _ => Err(SqlToCircuitError::Unsupported {
            reason: "non-literal".into(),
        }),
    }
}

fn extract_agg(
    select: &Select,
    node: NodeId,
) -> Result<
    (
        Vec<CircuitColumnRef>,
        Vec<CircuitAggFunc>,
        Vec<String>,
        Vec<(String, CircuitExpr)>,
    ),
    SqlToCircuitError,
> {
    let keys = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| col_ref(e, node))
            .collect::<Result<Vec<_>, _>>()?,
        _ => vec![],
    };
    let mut aliases = Vec::new();
    let mut funcs = Vec::new();
    let mut pre_computed = Vec::new();
    for item in &select.projection {
        let (func_opt, alias_opt) = match item {
            SelectItem::UnnamedExpr(Expr::Function(func)) if func.over.is_none() => {
                (Some(func), None)
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Function(func),
                alias,
            } if func.over.is_none() => (Some(func), Some(alias.value.clone())),
            SelectItem::UnnamedExpr(e) => {
                if let Ok(cr) = col_ref(e, node) {
                    aliases.push(cr.column_id.clone());
                }
                (None, None)
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Ok(_cr) = col_ref(expr, node) {
                    aliases.push(alias.value.clone());
                }
                (None, None)
            }
            _ => (None, None),
        };
        if let Some(func) = func_opt {
            let name = func.name.to_string().to_uppercase();

            let is_distinct = matches!(
                &func.args,
                sqlparser::ast::FunctionArguments::List(list)
                    if matches!(
                        list.duplicate_treatment,
                        Some(sqlparser::ast::DuplicateTreatment::Distinct)
                    )
            );

            let first_arg = match &func.args {
                sqlparser::ast::FunctionArguments::List(list) => list.args.iter().find_map(|a| {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = a
                    {
                        match e {
                            Expr::Identifier(i) if i.value.to_uppercase() == "DISTINCT" => None,
                            _ => Some(e),
                        }
                    } else {
                        None
                    }
                }),
                _ => None,
            };

            let is_simple_col = first_arg
                .map(|e| matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)))
                .unwrap_or(false);
            let first_arg_col = if is_simple_col {
                first_arg.and_then(|e| col_ref(e, node).ok())
            } else {
                None
            };

            let mut agg_col = if let Some(ref c) = first_arg_col {
                Some(c.clone())
            } else if let Some(arg_expr) = first_arg {
                if matches!(arg_expr, Expr::Value(_)) {
                    None
                } else {
                    let col_name = format!("__agg_arg_{}", pre_computed.len());
                    let circuit_expr = expr_to_circuit(arg_expr, node)?;
                    pre_computed.push((col_name.clone(), circuit_expr));
                    Some(CircuitColumnRef {
                        node_id: node,
                        column_id: col_name,
                    })
                }
            } else {
                None
            };

            if let Some(filter) = func.filter.as_deref() {
                let arg_expr = first_arg.ok_or_else(|| SqlToCircuitError::Unsupported {
                    reason: "filtered aggregate requires an explicit argument".into(),
                })?;
                let col_name = format!("__agg_arg_{}", pre_computed.len());
                pre_computed.push((
                    col_name.clone(),
                    CircuitExpr::If {
                        condition: Box::new(extract_filter_expr(filter, node)?),
                        then_expr: Box::new(expr_to_circuit(arg_expr, node)?),
                        else_expr: Box::new(CircuitExpr::Literal(serde_json::Value::Null)),
                    },
                ));
                agg_col = Some(CircuitColumnRef {
                    node_id: node,
                    column_id: col_name,
                });
            }

            match name.as_str() {
                "SUM" => {
                    if let Some(c) = agg_col {
                        funcs.push(CircuitAggFunc::Sum(c));
                    }
                }
                "COUNT" => {
                    if is_distinct {
                        if let Some(c) = agg_col {
                            funcs.push(CircuitAggFunc::CountDistinct(c));
                        } else {
                            funcs.push(CircuitAggFunc::Count);
                        }
                    } else {
                        funcs.push(CircuitAggFunc::Count);
                    }
                }
                "MIN" => {
                    if let Some(c) = agg_col {
                        funcs.push(CircuitAggFunc::Min(c));
                    }
                }
                "MAX" => {
                    if let Some(c) = agg_col {
                        funcs.push(CircuitAggFunc::Max(c));
                    }
                }
                "AVG" => {
                    if let Some(c) = agg_col {
                        funcs.push(CircuitAggFunc::Avg(c));
                    }
                }
                _ => {}
            }
            aliases.push(alias_opt.unwrap_or_else(|| name.to_lowercase()));
        }
    }
    Ok((keys, funcs, aliases, pre_computed))
}

fn extract_proj(
    select: &Select,
    node: NodeId,
) -> Result<(Vec<CircuitProjection>, Vec<(String, CircuitExpr)>), SqlToCircuitError> {
    let mut cols = Vec::new();
    let mut computed = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Function(func)) if func.over.is_some() => {
                let output_column_id = if func.name.to_string().eq_ignore_ascii_case("ROW_NUMBER") {
                    "row_number".to_string()
                } else {
                    func.name.to_string().to_lowercase()
                };
                cols.push(CircuitProjection {
                    source_column: CircuitColumnRef {
                        node_id: node,
                        column_id: output_column_id.clone(),
                    },
                    output_column_id,
                });
            }
            SelectItem::UnnamedExpr(e) => {
                match col_ref(e, node) {
                    Ok(cr) => cols.push(CircuitProjection {
                        source_column: cr.clone(),
                        output_column_id: cr.column_id,
                    }),
                    Err(_) => {
                        // Non-column expression: try to convert to CircuitExpr for Map node
                        match expr_to_circuit(e, node) {
                            Ok(expr) => {
                                let out_name = format!("__computed_{}", computed.len());
                                cols.push(CircuitProjection {
                                    source_column: CircuitColumnRef {
                                        node_id: node,
                                        column_id: out_name.clone(),
                                    },
                                    output_column_id: out_name.clone(),
                                });
                                computed.push((out_name, expr));
                            }
                            Err(_) => {}
                        }
                    }
                }
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Function(func),
                alias,
            } if func.over.is_some() => {
                cols.push(CircuitProjection {
                    source_column: CircuitColumnRef {
                        node_id: node,
                        column_id: alias.value.clone(),
                    },
                    output_column_id: alias.value.clone(),
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => match col_ref(expr, node) {
                Ok(cr) => cols.push(CircuitProjection {
                    source_column: cr,
                    output_column_id: alias.value.clone(),
                }),
                Err(_) => match expr_to_circuit(expr, node) {
                    Ok(circuit_expr) => {
                        let out_name = alias.value.clone();
                        cols.push(CircuitProjection {
                            source_column: CircuitColumnRef {
                                node_id: node,
                                column_id: out_name.clone(),
                            },
                            output_column_id: out_name.clone(),
                        });
                        computed.push((out_name, circuit_expr));
                    }
                    Err(_) => {}
                },
            },
            _ => {}
        }
    }
    Ok((cols, computed))
}

fn extract_join_keys(
    constraint: &JoinConstraint,
    left: NodeId,
    right: NodeId,
) -> Result<(CircuitColumnRef, CircuitColumnRef), SqlToCircuitError> {
    match constraint {
        JoinConstraint::On(Expr::BinaryOp {
            left: l,
            op: BinaryOperator::Eq,
            right: r,
        }) => Ok((col_ref(l, left)?, col_ref(r, right)?)),
        JoinConstraint::Using(attrs) if attrs.len() == 1 => {
            let obj_name = attrs.first().unwrap();
            // ObjectName wraps Vec<ObjectNamePart>, extract the first identifier
            let name = match obj_name.0.first() {
                Some(sqlparser::ast::ObjectNamePart::Identifier(i)) => i.value.clone(),
                _ => {
                    return Err(SqlToCircuitError::Unsupported {
                        reason: "USING requires simple column names".into(),
                    })
                }
            };
            Ok((
                CircuitColumnRef {
                    node_id: left,
                    column_id: name.clone(),
                },
                CircuitColumnRef {
                    node_id: right,
                    column_id: name,
                },
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
        TableSchema {
            name: name.into(),
            columns: cols.iter().map(|s| s.to_string()).collect(),
        }
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
        )
        .unwrap();
        assert!(c.nodes.len() >= 2);
    }

    #[test]
    fn select_with_group_by() {
        let c = sql_to_circuit(
            "SELECT dept, SUM(sal) FROM emp GROUP BY dept",
            &[tbl("emp", &["dept", "sal"])],
        )
        .unwrap();
        assert!(c.nodes.len() >= 2);
    }

    #[test]
    fn select_with_count_distinct() {
        let c = sql_to_circuit(
            "SELECT dept, COUNT(DISTINCT sal) FROM emp GROUP BY dept",
            &[tbl("emp", &["dept", "sal"])],
        )
        .unwrap();
        assert!(c.nodes.iter().any(|node| matches!(
            node,
            CircuitNode::Aggregate { functions, .. }
                if matches!(functions.as_slice(), [CircuitAggFunc::CountDistinct(column)] if column.column_id == "sal")
        )));
    }

    #[test]
    fn select_with_having() {
        let c = sql_to_circuit(
            "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5",
            &[tbl("emp", &["dept", "sal"])],
        )
        .unwrap();
        // Source → Aggregate → Filter (having)
        assert!(c.nodes.len() >= 3);
        assert!(c
            .nodes
            .iter()
            .any(|n| matches!(n, CircuitNode::Filter { .. })));
    }

    #[test]
    fn select_with_order_by_limit() {
        let c = sql_to_circuit(
            "SELECT name FROM users ORDER BY name LIMIT 10",
            &[tbl("users", &["id", "name"])],
        )
        .unwrap();
        // Source → TopK
        assert!(c.nodes.len() >= 2);
        assert!(c
            .nodes
            .iter()
            .any(|n| matches!(n, CircuitNode::TopK { limit: 10, .. })));
    }

    #[test]
    fn select_with_having_and_order_by() {
        let c = sql_to_circuit(
            "SELECT dept, SUM(sal) as total FROM emp GROUP BY dept HAVING SUM(sal) > 1000 ORDER BY total DESC LIMIT 5",
            &[tbl("emp", &["dept", "sal"])],
        ).unwrap();
        // Source → Aggregate → Filter → TopK
        assert!(c.nodes.len() >= 4);
        assert!(c
            .nodes
            .iter()
            .any(|n| matches!(n, CircuitNode::Aggregate { .. })));
        assert!(c
            .nodes
            .iter()
            .any(|n| matches!(n, CircuitNode::Filter { .. })));
        assert!(c
            .nodes
            .iter()
            .any(|n| matches!(n, CircuitNode::TopK { limit: 5, .. })));
    }

    #[test]
    fn select_order_by_aggregate_function() {
        let c = sql_to_circuit(
            "SELECT dept, SUM(sal) FROM emp GROUP BY dept ORDER BY SUM(sal) DESC",
            &[tbl("emp", &["dept", "sal"])],
        )
        .unwrap();
        // Source → Aggregate → TopK
        assert!(c
            .nodes
            .iter()
            .any(|n| matches!(n, CircuitNode::Aggregate { .. })));
        assert!(c.nodes.iter().any(|n| matches!(
            n,
            CircuitNode::TopK {
                descending: true,
                ..
            }
        )));
    }

    #[test]
    fn select_order_by_count_star() {
        let c = sql_to_circuit(
            "SELECT dept, COUNT(*) FROM emp GROUP BY dept ORDER BY COUNT(*) DESC LIMIT 3",
            &[tbl("emp", &["dept", "sal"])],
        )
        .unwrap();
        assert!(c.nodes.iter().any(|n| matches!(
            n,
            CircuitNode::TopK {
                limit: 3,
                descending: true,
                ..
            }
        )));
    }

    #[test]
    fn select_row_number_window_function() {
        let c = sql_to_circuit(
            "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY name) AS rn FROM emp",
            &[tbl("emp", &["name", "dept", "sal"])],
        )
        .unwrap();
        assert!(c
            .nodes
            .iter()
            .any(|n| matches!(n, CircuitNode::RowNumber { .. })));
    }

    #[test]
    fn select_row_number_no_partition() {
        let c = sql_to_circuit(
            "SELECT name, ROW_NUMBER() OVER (ORDER BY name DESC) AS rn FROM emp",
            &[tbl("emp", &["name"])],
        )
        .unwrap();
        assert!(c.nodes.iter().any(|n| matches!(
            n,
            CircuitNode::RowNumber {
                descending: true,
                ..
            }
        )));
    }

    #[test]
    fn select_having_order_by_limit_combined() {
        let c = sql_to_circuit(
            "SELECT dept, SUM(sal) as total, COUNT(*) as cnt FROM emp GROUP BY dept HAVING COUNT(*) > 3 ORDER BY total DESC, cnt ASC LIMIT 10",
            &[tbl("emp", &["dept", "sal"])],
        ).unwrap();
        assert!(c
            .nodes
            .iter()
            .any(|n| matches!(n, CircuitNode::Aggregate { .. })));
        assert!(c
            .nodes
            .iter()
            .any(|n| matches!(n, CircuitNode::Filter { .. })));
        assert!(c
            .nodes
            .iter()
            .any(|n| matches!(n, CircuitNode::TopK { limit: 10, .. })));
    }
}
