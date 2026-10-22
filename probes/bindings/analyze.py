"""Recompute the single-workload binding comparison from its three raw rounds."""
import hashlib
import json
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "results/binding-cost"
manifest = json.loads((OUT / "manifest.json").read_text())
for relative, expected in manifest["fingerprints"].items():
    assert hashlib.sha256((ROOT/relative).read_bytes()).hexdigest() == expected, relative
rows, file_hashes = [], {}
for i in range(3):
    p = OUT / f"round-{i}.json"
    data = json.loads(p.read_text())
    assert data["round"] == i and len(data["rows"]) == 6
    assert {(r["kind"], r["phase"]) for r in data["rows"]} == {
        (k, s) for k in ("PybindFixture", "CapiFixture") for s in ("receive", "return", "roundtrip")}
    for r in data["rows"]:
        assert r["batches"] == manifest["iterations"]
        assert r["items"] == manifest["batch"]*r["batches"]
        assert r["cpu_ns_per_item"] == r["cpu_ns"]/r["items"]
        assert r["wall_ns"] > 0 and r["cpu_ns"] > 0
        if r["phase"] != "receive":
            assert r["accepted_calls"] == r["batches"]+5000
            assert r["accepted_items"] == r["accepted_calls"]*6
        rows.append({"round":i, **r})
    file_hashes[p.name] = hashlib.sha256(p.read_bytes()).hexdigest()
summary = {}
for phase in ("receive", "return", "roundtrip"):
    summary[phase] = {}
    for kind in ("PybindFixture", "CapiFixture"):
        values = [r["cpu_ns_per_item"] for r in rows if r["kind"] == kind and r["phase"] == phase]
        summary[phase][kind] = dict(rounds_ns_per_item=values, median_ns_per_item=median(values))
    py = summary[phase]["PybindFixture"]["median_ns_per_item"]
    c = summary[phase]["CapiFixture"]["median_ns_per_item"]
    summary[phase].update(saved_ns_per_item=py-c, pybind_over_capi=py/c, cpu_reduction=(py-c)/py)
summary["sum_of_receive_return_medians"] = {
    k:sum(summary[s][k]["median_ns_per_item"] for s in ("receive", "return"))
    for k in ("PybindFixture", "CapiFixture")}
summary["project_correctness"] = json.loads((ROOT/"results/binding-correctness/summary.json").read_text())
(OUT/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
(OUT/"verification.json").write_text(json.dumps(dict(raw_rows=len(rows), fingerprints_match=True,
    raw_hashes=file_hashes, checks="batch sizes, conversion counters, output data checked in runner/tests; CPU durations reconciled"),indent=2)+"\n")
print(json.dumps(summary,indent=2))
