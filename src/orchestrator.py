"""A tiny DAG orchestrator — the Airflow/Dagster idea in ~50 lines.

Tasks declare upstream dependencies; the scheduler topologically sorts them,
runs each once its parents have succeeded, records status + duration, and stops
a branch if an upstream task fails. This is what an orchestrator does for you.
"""
import time


class Task:
    def __init__(self, name, fn, deps=None):
        self.name = name
        self.fn = fn
        self.deps = deps or []


class DAG:
    def __init__(self):
        self.tasks = {}

    def add(self, name, fn, deps=None):
        self.tasks[name] = Task(name, fn, deps)
        return self

    def _toposort(self):
        order, seen, temp = [], set(), set()

        def visit(n):
            if n in seen:
                return
            if n in temp:
                raise ValueError(f"cycle through {n}")
            temp.add(n)
            for d in self.tasks[n].deps:
                visit(d)
            temp.discard(n); seen.add(n); order.append(n)
        for n in self.tasks:
            visit(n)
        return order

    def run(self):
        order = self._toposort()
        status, log = {}, []
        for name in order:
            t = self.tasks[name]
            if any(status.get(d) != "success" for d in t.deps):
                status[name] = "skipped"
                log.append({"task": name, "status": "skipped", "seconds": 0.0})
                continue
            t0 = time.perf_counter()
            try:
                t.fn()
                status[name] = "success"
                log.append({"task": name, "status": "success",
                            "seconds": round(time.perf_counter() - t0, 3)})
            except Exception as e:                       # noqa: BLE001
                status[name] = "failed"
                log.append({"task": name, "status": "failed",
                            "seconds": round(time.perf_counter() - t0, 3),
                            "error": str(e)})
        return order, log
