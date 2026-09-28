"""Great-Expectations-style data-quality tests against the warehouse.

Each test is a SQL query returning a count of violations; zero means pass. These
run *between* transformation steps so bad data fails the pipeline loudly instead
of silently corrupting a dashboard.
"""


def _scalar(conn, sql):
    return conn.execute(sql).fetchone()[0]


def not_null(conn, table, col):
    bad = _scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
    return bad == 0, f"{table}.{col} not_null: {bad} nulls"


def unique(conn, table, col):
    bad = _scalar(conn, f"SELECT COUNT(*) FROM (SELECT {col} FROM {table} "
                        f"GROUP BY {col} HAVING COUNT(*) > 1)")
    return bad == 0, f"{table}.{col} unique: {bad} duplicated keys"


def accepted_values(conn, table, col, values):
    inlist = ",".join(f"'{v}'" for v in values)
    bad = _scalar(conn, f"SELECT COUNT(*) FROM {table} "
                        f"WHERE {col} NOT IN ({inlist})")
    return bad == 0, f"{table}.{col} accepted_values: {bad} out-of-range"


def relationships(conn, table, col, ref_table, ref_col):
    bad = _scalar(conn, f"SELECT COUNT(*) FROM {table} t "
                        f"LEFT JOIN {ref_table} r ON t.{col}=r.{ref_col} "
                        f"WHERE r.{ref_col} IS NULL")
    return bad == 0, f"{table}.{col} -> {ref_table}.{ref_col}: {bad} orphans"


def row_count_positive(conn, table):
    n = _scalar(conn, f"SELECT COUNT(*) FROM {table}")
    return n > 0, f"{table} row_count: {n} rows"


def run_suite(conn, suite):
    """suite: list of (callable, args...). Returns list of result dicts."""
    results = []
    for fn, *args in suite:
        ok, msg = fn(conn, *args)
        results.append({"test": fn.__name__, "passed": ok, "detail": msg})
    return results
