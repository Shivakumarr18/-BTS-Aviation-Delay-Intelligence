# ADR-010: FLIGHTS Column Dropped in Silver Layer

## Context

BTS source data contains a FLIGHTS column
present in all 36 monthly CSV files.

## Validation

Validated across full 20,928,599 rows
using quick_check.py — August 4, 2026:

| Metric          | Result     |
| --------------- | ---------- |
| Distinct values | 1          |
| Min value       | 1.0        |
| Max value       | 1.0        |
| NULL count      | 0          |
| Total rows      | 20,928,599 |

## Decision

Drop FLIGHTS column in Silver transformation.
Bronze preserves it (append-only principle —
Bronze never modifies source data).
Silver drops it with this documented reason.

## Reason

Column is a constant — always = 1.0.
At our grain (one row = one scheduled flight),
SUM(FLIGHTS) = COUNT(\*) in every query.
The column adds zero analytical value and
consumes storage across 20.9M rows.

## Consequence

Bronze: 37 columns (original preserved)
Silver: 36 columns (FLIGHTS dropped)
No analytical capability lost.
Fully documented and reversible.
