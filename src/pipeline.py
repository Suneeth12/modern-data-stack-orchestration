"""End-to-end modern-data-stack run: ingest -> staging -> marts, with tests.

Mirrors a dbt + orchestrator + Great-Expectations workflow on a local SQLite
'warehouse':
  * ingest        — load raw CSVs into the warehouse
  * test_raw      — profile the raw data (informational; shows the dirtiness)
  * stg_*         — dbt-style staging models that CLEAN the raw tables
  * test_staging  — enforce contracts on the cleaned tables (fails the run if bad)
  * mart_*        — aggregated business tables
  * test_marts    — enforce contracts on the marts

The DAG orchestrator runs the tasks in dependency order and records a run report.
"""
import json
import sqlite3
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from orchestrator import DAG
import quality as q

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
DB = DATA / "warehouse.db"

# dbt-style SQL models (staging cleans; marts aggregate)
MODELS = {
    "stg_customers": """
        CREATE TABLE stg_customers AS
        SELECT customer_id,
               CASE WHEN region IS NULL OR region = '' THEN 'Unknown' ELSE region END AS region,
               signup_year
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_year) rn
              FROM raw_customers)
        WHERE rn = 1;""",
    "stg_orders": """
        CREATE TABLE stg_orders AS
        SELECT order_id, customer_id, CAST(amount AS REAL) AS amount, status
        FROM raw_orders
        WHERE amount IS NOT NULL AND amount != '' AND status IN ('paid','refunded','pending');""",
    "mart_customer_revenue": """
        CREATE TABLE mart_customer_revenue AS
        SELECT c.customer_id, c.region,
               COUNT(o.order_id) AS n_orders,
               COALESCE(SUM(CASE WHEN o.status='paid' THEN o.amount END), 0) AS revenue
        FROM stg_customers c
        LEFT JOIN stg_orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.region;""",
    "mart_region_summary": """
        CREATE TABLE mart_region_summary AS
        SELECT region, COUNT(*) AS n_customers, ROUND(SUM(revenue), 2) AS revenue
        FROM mart_customer_revenue GROUP BY region ORDER BY revenue DESC;""",
}

LAYER = {"ingest": 0, "test_raw": 1, "stg_customers": 1, "stg_orders": 1,
         "test_staging": 2, "mart_customer_revenue": 2,
         "mart_region_summary": 3, "test_marts": 3}


def build():
    DB.unlink(missing_ok=True)
    conn = sqlite3.connect(DB)
    quality_results = {}

    def ingest():
        for name in ["raw_customers", "raw_orders"]:
            df = pd.read_csv(DATA / f"{name}.csv")
            df.to_sql(name, conn, if_exists="replace", index=False)

    def run_model(key):
        def _fn():
            conn.executescript(f"DROP TABLE IF EXISTS {key}; {MODELS[key]}")
            conn.commit()
        return _fn

    def test_raw():
        quality_results["raw"] = q.run_suite(conn, [
            (q.unique, "raw_customers", "customer_id"),
            (q.not_null, "raw_orders", "amount"),
            (q.relationships, "raw_orders", "customer_id", "raw_customers", "customer_id"),
        ])  # informational: do not raise

    def test_staging():
        res = q.run_suite(conn, [
            (q.unique, "stg_customers", "customer_id"),
            (q.not_null, "stg_customers", "region"),
            (q.accepted_values, "stg_orders", "status", ["paid", "refunded", "pending"]),
            (q.not_null, "stg_orders", "amount"),
            (q.row_count_positive, "stg_orders"),
        ])
        quality_results["staging"] = res
        _enforce(res)

    def test_marts():
        res = q.run_suite(conn, [
            (q.unique, "mart_customer_revenue", "customer_id"),
            (q.not_null, "mart_region_summary", "region"),
            (q.row_count_positive, "mart_region_summary"),
        ])
        quality_results["marts"] = res
        _enforce(res)

    dag = DAG()
    dag.add("ingest", ingest)
    dag.add("test_raw", test_raw, deps=["ingest"])
    dag.add("stg_customers", run_model("stg_customers"), deps=["ingest"])
    dag.add("stg_orders", run_model("stg_orders"), deps=["ingest"])
    dag.add("test_staging", test_staging, deps=["stg_customers", "stg_orders"])
    dag.add("mart_customer_revenue", run_model("mart_customer_revenue"),
            deps=["test_staging"])
    dag.add("mart_region_summary", run_model("mart_region_summary"),
            deps=["mart_customer_revenue"])
    dag.add("test_marts", test_marts, deps=["mart_region_summary"])
    return dag, conn, quality_results


def _enforce(results):
    failed = [r for r in results if not r["passed"]]
    if failed:
        raise AssertionError("; ".join(r["detail"] for r in failed))


def lineage_plot(order, dag):
    fig, ax = plt.subplots(figsize=(9, 4.5))
    layers = {}
    for n in order:
        layers.setdefault(LAYER[n], []).append(n)
    pos = {}
    for lx, nodes in layers.items():
        for i, n in enumerate(nodes):
            pos[n] = (lx, i - len(nodes) / 2)
    for n in order:                                  # dependency arrows
        for d in dag.tasks[n].deps:
            x0, y0 = pos[d]; x1, y1 = pos[n]
            ax.annotate("", xy=(x1, y1), xytext=(x0, y0), zorder=1,
                        arrowprops=dict(arrowstyle="->", color="#bbb", lw=1.2))
    for n in order:
        x, y = pos[n]
        color = "#e76f51" if n.startswith("test") else "#2a9d8f"
        ax.scatter([x], [y], s=2400, color=color, zorder=2)
        ax.text(x, y, n.replace("_", "\n"), ha="center", va="center",
                fontsize=7, color="white", zorder=3)
    ax.set_axis_off(); ax.set_title("Pipeline DAG (green = model, orange = test)")
    fig.tight_layout(); fig.savefig(REPORTS / "lineage.png", dpi=110)


def main():
    dag, conn, quality_results = build()
    order, log = dag.run()
    REPORTS.mkdir(exist_ok=True)

    region = pd.read_sql("SELECT * FROM mart_region_summary", conn)
    report = {"execution_order": order, "task_log": log,
              "quality": quality_results,
              "mart_region_summary": region.to_dict(orient="records")}
    (REPORTS / "run_report.json").write_text(json.dumps(report, indent=2))
    lineage_plot(order, dag)

    print("DAG order:", " -> ".join(order))
    for r in log:
        print(f"  {r['task']:24s} {r['status']:8s} {r['seconds']}s")
    raw_fail = sum(not t["passed"] for t in quality_results.get("raw", []))
    print(f"raw data issues caught: {raw_fail} | staging/marts tests: all enforced & passed")
    print("\nRevenue by region:")
    print(region.to_string(index=False))
    print("\nSee reports/lineage.png and reports/run_report.json")
    conn.close()


if __name__ == "__main__":
    main()
