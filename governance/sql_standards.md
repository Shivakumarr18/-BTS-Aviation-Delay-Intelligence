SQL Engineering Standards
Narsing Shiva Kumar | Production SQL Guidelines

1. Scale Mindset

Write every query as if it will run on 100M+ rows.

Design for scalability from the first draft, not after performance issues appear.

2. Filter Early

Reduce data as early as possible before expensive operations such as:

JOIN
GROUP BY
WINDOW FUNCTIONS
SORT

Filter only when business logic allows.

3. Write SARGable Predicates

Never wrap indexed columns in functions inside the WHERE clause.

❌ Avoid

WHERE YEAR(order_date) = 2024

✅ Prefer

WHERE order_date >= '2024-01-01'
AND order_date < '2025-01-01'

Enable index usage and partition pruning whenever possible.

4. Analyze Distributions, Not Just Averages

Average alone can hide important variation.

When variability matters, accompany AVG() with metrics such as:

STDDEV()
PERCENTILE_CONT()
MIN()
MAX()

Always understand the distribution before drawing conclusions.

5. Respect NULLs

Every NULL should have a documented business meaning.

Never replace NULL with 0 without justification.
Document what NULL represents for each column.
Preserve business semantics throughout the pipeline. 6. Window Functions

Use window functions intentionally.

Define window frames explicitly for aggregate windows.
Build complete date spines before calculating moving averages.
Partition by business logic—not convenience. 7. Prefer Readable SQL

Use CTEs whenever they improve readability and maintainability.

Good SQL should be easy to:

Read
Debug
Review
Modify

Validate execution plans instead of assuming CTEs improve performance.

8. Select Only Required Columns

Never use:

SELECT \*

in analytical or production queries.

Return only the columns required by downstream consumers.

9. COUNT with Intention

Use the appropriate COUNT function for the question being answered.

COUNT(\*) → Total rows

COUNT(column) → Non-NULL values

COUNT(DISTINCT) → Unique values 10. Meaningful Aliases

Aliases should communicate business meaning.

Good:

total_delayed_flights

average_arrival_delay_minutes

carrier_delay_percentage

Avoid:

col1

avg1

temp

11. JOIN Discipline

Choose JOIN types deliberately.

Always understand why the query requires:

INNER JOIN
LEFT JOIN
RIGHT JOIN
FULL OUTER JOIN

Avoid unintended Cartesian products.

12. Validate Execution Plans

Before production execution:

Use EXPLAIN
Use EXPLAIN ANALYZE where appropriate

Understand:

Join strategy
Scan type
Index usage
Partition pruning
Estimated vs actual rows

Never optimize blindly.

13. Deterministic Ordering

Never rely on implicit row ordering.

Whenever result order matters:

ORDER BY flight_date,
flight_id;

14. Respect Data Types:

Avoid unnecessary implicit conversions.

Join compatible data types.
Cast explicitly when required.
Prevent unnecessary full table scans caused by implicit casting.

15. Validate Transformations:

Every transformation should be verifiable.

Examples include:

Row counts
Duplicate counts
NULL counts
Business rule validation
Aggregate sanity checks

Trust results only after validation.

**Engineering Philosophy: **

SQL should be correct before fast, readable before clever, and scalable before necessary. Every query should preserve business meaning while remaining understandable, maintainable, and efficient at production scale.
