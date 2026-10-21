//! Conservative scope inference for an ordinary, already relation-bound query.
//! Wire as a child of query_serving. None means full dependency closure, never
//! rejection of otherwise supported query SQL. This is not SQL admission.
use super::{query_bind_value_to_sql_literal, QueryBindValue, RelationSchema};
use sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
use std::ops::ControlFlow;

pub(super) fn infer_scope(
    bound_sql: &str,
    binds: &[QueryBindValue],
    output_schema: &RelationSchema,
) -> Option<String> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, bound_sql).ok()?;
    let [Statement::Query(query)] = statements.as_mut_slice() else {
        return None;
    };
    if query.with.is_some()
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
    if select.distinct.is_some()
        || select.into.is_some()
        || select.top.is_some()
        || select.select_modifiers.is_some()
        || select.exclude.is_some()
        || !select.optimizer_hints.is_empty()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || !matches!(&select.group_by, GroupByExpr::Expressions(keys, modifiers) if keys.is_empty() && modifiers.is_empty())
    {
        return None;
    }
    select.selection.as_ref()?;
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
    if table.value != output_schema.relation_id
        || !with_hints.is_empty()
        || !partitions.is_empty()
        || !index_hints.is_empty()
        || alias.as_ref().is_some_and(|a| !a.columns.is_empty())
    {
        return None;
    }
    let qualifier = alias
        .as_ref()
        .map_or_else(|| table.value.clone(), |a| a.name.value.clone());
    for projection in &select.projection {
        match projection {
            SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => {}
            SelectItem::Wildcard(options) if options.to_string().is_empty() => {}
            SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(name),
                options,
            ) if options.to_string().is_empty()
                && matches!(name.0.as_slice(),
                    [ObjectNamePart::Identifier(id)] if id.value == qualifier) => {}
            _ => return None,
        }
    }
    let mut normalizer = ScopeNormalizer {
        schema: output_schema,
        qualifier,
        binds,
        queries: 0,
    };
    if VisitMut::visit(query.as_mut(), &mut normalizer).is_break() {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    Some(select.selection.as_ref()?.to_string())
}

struct ScopeNormalizer<'a> {
    schema: &'a RelationSchema,
    qualifier: String,
    binds: &'a [QueryBindValue],
    queries: usize,
}

impl ScopeNormalizer<'_> {
    fn column(&self, id: &Ident) -> bool {
        // Exact names deliberately avoid guessing engine-specific case folding.
        self.schema
            .columns
            .iter()
            .any(|column| column.name == id.value)
    }

    fn literal(&self, placeholder: &str) -> Option<Expr> {
        let index = placeholder
            .strip_prefix('$')?
            .parse::<usize>()
            .ok()?
            .checked_sub(1)?;
        let value = self.binds.get(index)?;
        // Float64 formatting can produce an integer literal (notably near 2^53),
        // changing comparison coercion and excluding matching Int64 rows.
        // Decimal strings and composite values also need typed preservation.
        if !matches!(
            value,
            QueryBindValue::Utf8(_)
                | QueryBindValue::Int64(_)
                | QueryBindValue::Boolean(_)
                | QueryBindValue::Date(_)
                | QueryBindValue::Time(_)
                | QueryBindValue::Timestamp(_)
                | QueryBindValue::Uuid(_)
        ) {
            return None;
        }
        let literal = query_bind_value_to_sql_literal(value).ok()?;
        // Date/Time/Timestamp bind variants currently execute as Utf8 scalars
        // in runtime_contract. Quoted strings preserve that binding behavior;
        // any explicit temporal CAST remains in the surrounding query AST.
        let mut statements =
            Parser::parse_sql(&GenericDialect {}, &format!("SELECT {literal}")).ok()?;
        let [Statement::Query(query)] = statements.as_mut_slice() else {
            return None;
        };
        let SetExpr::Select(select) = query.body.as_mut() else {
            return None;
        };
        let [SelectItem::UnnamedExpr(expr)] = select.projection.as_slice() else {
            return None;
        };
        Some(expr.clone())
    }
}

impl VisitorMut for ScopeNormalizer<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
        self.queries += 1;
        if self.queries == 1 {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(id) if self.column(id) => {}
            Expr::CompoundIdentifier(ids) => {
                let [qualifier, column] = ids.as_slice() else {
                    return ControlFlow::Break(());
                };
                if qualifier.value != self.qualifier || !self.column(column) {
                    return ControlFlow::Break(());
                }
                *expr = Expr::Identifier(column.clone());
            }
            Expr::Value(value) => {
                if let Value::Placeholder(placeholder) = &value.value {
                    let Some(literal) = self.literal(placeholder) else {
                        return ControlFlow::Break(());
                    };
                    *expr = literal;
                }
            }
            Expr::TypedString(_)
            | Expr::Nested(_)
            | Expr::BinaryOp { .. }
            | Expr::UnaryOp { .. }
            | Expr::Cast { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Between { .. }
            | Expr::InList { .. } => {}
            // In particular, reject every function (aggregate/window/volatile),
            // subquery, and expression family not explicitly reviewed above.
            _ => return ControlFlow::Break(()),
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velorix_core::view_contract::{ColumnSchema, SqlDataType};
    fn schema() -> RelationSchema {
        RelationSchema {
            relation_id: "measurements".into(),
            relation_name: "measurements".into(),
            relation_version: "v1".into(),
            schema_fingerprint: "schema".into(),
            primary_key: vec![],
            columns: vec![
                ColumnSchema {
                    name: "time_ns".into(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "label".into(),
                    data_type: SqlDataType::Utf8,
                    nullable: true,
                },
            ],
        }
    }
    #[test]
    fn literal_range_and_qualified_bound_values_are_self_contained() {
        assert_eq!(
            infer_scope(
                "SELECT * FROM measurements WHERE time_ns >= 10 AND time_ns < 20",
                &[],
                &schema()
            ),
            Some("time_ns >= 10 AND time_ns < 20".into())
        );
        assert_eq!(
            infer_scope(
                "SELECT m.time_ns FROM measurements AS m WHERE m.time_ns >= $1 AND m.label = $2",
                &[
                    QueryBindValue::Int64(10),
                    QueryBindValue::Utf8("a'b".into())
                ],
                &schema()
            ),
            Some("time_ns >= 10 AND label = 'a''b'".into())
        );
    }
    #[test]
    fn integral_float_bind_near_exact_integer_boundary_uses_full_closure() {
        assert!(infer_scope(
            "SELECT * FROM measurements WHERE time_ns = $1",
            &[QueryBindValue::Float64(9_007_199_254_740_992.0)],
            &schema(),
        )
        .is_none());
    }

    #[test]
    fn temporal_binds_keep_runtime_string_binding_and_explicit_casts() {
        for (value, target, literal) in [
            (
                QueryBindValue::Date("2024-01-02".into()),
                "DATE",
                "2024-01-02",
            ),
            (QueryBindValue::Time("12:30:00".into()), "TIME", "12:30:00"),
            (
                QueryBindValue::Timestamp("2024-01-02 12:30:00".into()),
                "TIMESTAMP",
                "2024-01-02 12:30:00",
            ),
        ] {
            let sql = format!(
                "SELECT * FROM measurements WHERE CAST(label AS {target}) = CAST($1 AS {target})"
            );
            assert_eq!(
                infer_scope(&sql, &[value], &schema()),
                Some(format!(
                    "CAST(label AS {target}) = CAST('{literal}' AS {target})"
                ))
            );
        }
    }

    #[test]
    fn unsafe_or_unresolved_shapes_use_full_closure() {
        for sql in [
            "SELECT count(*) FROM measurements WHERE time_ns > 1",
            "SELECT lag(time_ns) OVER (ORDER BY time_ns) FROM measurements WHERE time_ns > 1",
            "SELECT * FROM measurements WHERE time_ns IN (SELECT time_ns FROM measurements)",
            "SELECT * FROM measurements m JOIN measurements n ON m.time_ns=n.time_ns WHERE m.time_ns > 1",
            "SELECT * FROM measurements WHERE time_ns > $1",
            "SELECT * FROM measurements WHERE other.time_ns > 1",
            "SELECT * FROM measurements",
            "SELECT * FROM other WHERE time_ns > 1",
        ] { assert!(infer_scope(sql, &[], &schema()).is_none(), "{sql}"); }
    }
}
