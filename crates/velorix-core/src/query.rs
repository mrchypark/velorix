use std::error::Error as StdError;

use arrow::error::ArrowError;
use thiserror::Error;

pub use crate::resource_policy::{QueryExecutionPolicyV1, QueryPolicy, QueryPolicyError};

pub const INPUT_TABLE_NAME: &str = "input";

/// Extract an outer filter only when the entire AST reads one ordinary base table.
pub fn materialized_page_predicate(
    sql: &str,
    schema: &crate::view_contract::RelationSchema,
    binds: &[QueryBindValue],
) -> Option<PagePredicate> {
    use sqlparser::ast::*;
    use sqlparser::{dialect::GenericDialect, parser::Parser};
    use std::ops::ControlFlow;
    struct SingleQuery(usize);
    impl Visitor for SingleQuery {
        type Break = ();
        fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
            self.0 += 1;
            if self.0 > 1 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    let statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return None;
    };
    if query.visit(&mut SingleQuery(0)).is_break()
        || query.with.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return None;
    }
    let [from] = select.from.as_slice() else {
        return None;
    };
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name,
        alias,
        args: None,
        with_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
    } = &from.relation
    else {
        return None;
    };
    let [ObjectNamePart::Identifier(table)] = name.0.as_slice() else {
        return None;
    };
    if table.value != schema.relation_id
        || !with_hints.is_empty()
        || !partitions.is_empty()
        || !index_hints.is_empty()
        || alias.as_ref().is_some_and(|a| !a.columns.is_empty())
    {
        return None;
    }
    let predicate = select.selection.as_ref()?;
    Some(extract_predicate(
        predicate,
        schema,
        alias
            .as_ref()
            .map_or(table.value.as_str(), |a| a.name.value.as_str()),
        binds,
    ))
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum PageScalar {
    Utf8(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Timestamp(i64),
}

fn timestamp_nanos(text: &str) -> Option<i64> {
    use arrow::array::{Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, TimeUnit};
    let bytes = text.as_bytes();
    if bytes.len() < 19
        || bytes.len() > 29
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b' ' | b'T')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    if !(0..19)
        .filter(|i| ![4, 7, 10, 13, 16].contains(i))
        .all(|i| bytes[i].is_ascii_digit())
    {
        return None;
    }
    if bytes.len() > 19
        && (bytes.len() < 21 || bytes[19] != b'.' || !bytes[20..].iter().all(u8::is_ascii_digit))
    {
        return None;
    }
    if &bytes[17..19] >= b"60".as_slice() {
        return None;
    }
    let input = StringArray::from(vec![text]);
    let output =
        arrow::compute::cast(&input, &DataType::Timestamp(TimeUnit::Nanosecond, None)).ok()?;
    let output = output.as_any().downcast_ref::<TimestampNanosecondArray>()?;
    (!output.is_null(0)).then(|| output.value(0))
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PageComparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PagePredicate {
    Unknown,
    Compare {
        column: String,
        op: PageComparison,
        value: PageScalar,
    },
    Null {
        column: String,
        negated: bool,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

fn extract_predicate(
    expr: &sqlparser::ast::Expr,
    schema: &crate::view_contract::RelationSchema,
    qualifier: &str,
    binds: &[QueryBindValue],
) -> PagePredicate {
    use crate::view_contract::SqlDataType;
    use sqlparser::ast::{BinaryOperator as Op, Expr, UnaryOperator, Value};
    use PageComparison as C;
    use PagePredicate as P;
    let column = |e: &Expr| {
        let name = match e {
            Expr::Identifier(i) => &i.value,
            Expr::CompoundIdentifier(p) if p.len() == 2 && p[0].value == qualifier => &p[1].value,
            _ => return None,
        };
        schema.columns.iter().find(|c| c.name == *name)
    };
    fn literal(e: &Expr, binds: &[QueryBindValue]) -> Option<PageScalar> {
        match e {
            Expr::Value(v) => match &v.value {
                Value::SingleQuotedString(s) => Some(PageScalar::Utf8(s.clone())),
                Value::Boolean(b) => Some(PageScalar::Bool(*b)),
                Value::Number(n, _) => n.parse::<i64>().ok().map(PageScalar::Int).or_else(|| {
                    n.parse::<f64>()
                        .ok()
                        .filter(|n| n.is_finite() && n.abs() <= (1u64 << 53) as f64)
                        .map(PageScalar::Float)
                }),
                Value::Placeholder(p) => {
                    match binds.get(p.strip_prefix('$')?.parse::<usize>().ok()?.checked_sub(1)?)? {
                        QueryBindValue::Utf8(s) => Some(PageScalar::Utf8(s.clone())),
                        QueryBindValue::Int64(n) => Some(PageScalar::Int(*n)),
                        QueryBindValue::Float64(n) if n.is_finite() => Some(PageScalar::Float(*n)),
                        QueryBindValue::Boolean(b) => Some(PageScalar::Bool(*b)),
                        QueryBindValue::Timestamp(t) => {
                            timestamp_nanos(t).map(PageScalar::Timestamp)
                        }
                        _ => None,
                    }
                }
                _ => None,
            },
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => match literal(expr, binds)? {
                PageScalar::Int(n) => n.checked_neg().map(PageScalar::Int),
                PageScalar::Float(n) => Some(PageScalar::Float(-n)),
                _ => None,
            },
            Expr::TypedString(t)
                if matches!(
                    t.data_type,
                    sqlparser::ast::DataType::Timestamp(
                        None,
                        sqlparser::ast::TimezoneInfo::None
                            | sqlparser::ast::TimezoneInfo::WithoutTimeZone
                    )
                ) =>
            {
                match &t.value.value {
                    Value::SingleQuotedString(t) => timestamp_nanos(t).map(PageScalar::Timestamp),
                    _ => None,
                }
            }
            _ => None,
        }
    }
    let comparison = |e: &Expr, v: &Expr, op| {
        let Some(c) = column(e) else {
            return P::Unknown;
        };
        let Some(mut value) = literal(v, binds) else {
            return P::Unknown;
        };
        match (&c.data_type, &value) {
            (SqlDataType::Utf8, PageScalar::Utf8(_)) | (SqlDataType::Bool, PageScalar::Bool(_)) => {
            }
            (
                SqlDataType::Int8
                | SqlDataType::Int16
                | SqlDataType::Int32
                | SqlDataType::Int64
                | SqlDataType::UInt8
                | SqlDataType::UInt16
                | SqlDataType::UInt32,
                PageScalar::Int(_),
            ) => (),
            (SqlDataType::Float64, PageScalar::Float(_)) => (),
            (SqlDataType::Timestamp { timezone: None }, PageScalar::Timestamp(_)) => (),
            (SqlDataType::Float64, PageScalar::Int(n)) if n.unsigned_abs() <= 1u64 << 53 => {
                value = PageScalar::Float(*n as f64)
            }
            _ => return P::Unknown,
        }
        P::Compare {
            column: c.name.clone(),
            op,
            value,
        }
    };
    let recurse = |e| extract_predicate(e, schema, qualifier, binds);
    match expr {
        Expr::Nested(e) => recurse(e),
        Expr::BinaryOp {
            left,
            op: Op::And,
            right,
        } => P::And(Box::new(recurse(left)), Box::new(recurse(right))),
        Expr::BinaryOp {
            left,
            op: Op::Or,
            right,
        } => P::Or(Box::new(recurse(left)), Box::new(recurse(right))),
        Expr::BinaryOp { left, op, right } => {
            let (op, reverse) = match op {
                Op::Eq => (C::Eq, C::Eq),
                Op::NotEq => (C::Ne, C::Ne),
                Op::Lt => (C::Lt, C::Gt),
                Op::LtEq => (C::Le, C::Ge),
                Op::Gt => (C::Gt, C::Lt),
                Op::GtEq => (C::Ge, C::Le),
                _ => return P::Unknown,
            };
            if column(left).is_some() {
                comparison(left, right, op)
            } else {
                comparison(right, left, reverse)
            }
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => column(e).map_or(P::Unknown, |c| P::Null {
            column: c.name.clone(),
            negated: matches!(expr, Expr::IsNotNull(_)),
        }),
        Expr::InList {
            expr,
            list,
            negated: false,
        } if !list.is_empty() => list
            .iter()
            .map(|v| comparison(expr, v, C::Eq))
            .reduce(|a, b| P::Or(Box::new(a), Box::new(b)))
            .unwrap_or(P::Unknown),
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => {
            let lower = comparison(expr, low, C::Ge);
            let upper = comparison(expr, high, C::Le);
            // BETWEEN coerces all three operands together, not each bound separately.
            if matches!(lower, P::Unknown) || matches!(upper, P::Unknown) {
                P::Unknown
            } else {
                P::And(Box::new(lower), Box::new(upper))
            }
        }
        // shortcut: NOT and uncertain coercions read all pages; add typed three-valued proofs before pruning them.
        _ => P::Unknown,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryBindValue {
    Utf8(String),
    Json(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Date(String),
    Time(String),
    Timestamp(String),
    Uuid(String),
    Decimal(String),
    Binary(Vec<u8>),
    Utf8Array(Vec<String>),
    JsonArray(Vec<String>),
    Int64Array(Vec<i64>),
    Float64Array(Vec<f64>),
    BooleanArray(Vec<bool>),
    DateArray(Vec<String>),
    TimeArray(Vec<String>),
    TimestampArray(Vec<String>),
    UuidArray(Vec<String>),
    DecimalArray(Vec<String>),
    BinaryArray(Vec<Vec<u8>>),
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    Engine(#[from] Box<dyn StdError + Send + Sync>),
    #[error(transparent)]
    Policy(#[from] QueryPolicyError),
}

impl QueryError {
    pub fn engine(error: impl StdError + Send + Sync + 'static) -> Self {
        Self::Engine(Box::new(error))
    }
}
