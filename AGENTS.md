## Generic Query and View Support

When the user asks for generic SQL query or generic materialized view support,
do not treat another hardcoded SQL-shape runtime slice as completion.

The target architecture is a Feldera compiler-backed dynamic view pipeline:
registered Velorix relations plus user-provided Feldera SQL should be compiled
through Feldera, bound to executable artifact metadata, loaded by Velorix, and
run as a materialized view runtime. A narrow SQL fixture is acceptable only as a
test case for that pipeline, not as the product implementation.

Completion for generic query/view support requires evidence that the pipeline
handles multiple relation schemas and more than one SQL family. At minimum,
verify filters, projections, group by, aggregates such as sum/count/min/max/avg,
and a two-table join through the same compiler-backed path. Unsupported SQL must
return compiler or admission errors rather than silently falling back to a fake
generic implementation.

Do not expand Velorix-owned SQL lowering by adding one-off parser branches for
each requested example unless the user explicitly asks for a temporary prototype.
If Feldera compiler integration is not available yet, state that as the blocker
and implement the compiler integration boundary instead of adding another
example-specific generated runtime.
