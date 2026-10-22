"""Validate completeness and produce exact comparison tables from the final runs."""
import hashlib
import json
from pathlib import Path
import statistics as st

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {
    "fixed": ["single", "multiprocess", "seastar-batch", "seastar-policy", "native", "seastar-policy1", "native1"],
    "file": ["multiprocess", "multiprocess-asyncio", "seastar-batch", "seastar-policy", "native"],
    "file-shared": ["seastar-coalesced", "seastar-shared", "native", "multiprocess-asyncio"],
}


def summarize():
    report = {"workloads": {}, "validated_responses": 0, "validated_runs": 0}
    for workload, modes in EXPECTED.items():
        folder = ROOT / "results" / ("final-"+workload)
        manifest = json.loads((folder/"manifest.json").read_text())
        for relative, digest in manifest["fingerprints"].items():
            assert hashlib.sha256((ROOT/relative).read_bytes()).hexdigest() == digest, relative
        rows = [json.loads(p.read_text()) for p in folder.glob("*-client5.json")]
        assert len(rows) == 3*len(modes)
        output = {}
        for mode in modes:
            runs = sorted((r for r in rows if r["mode"] == mode), key=lambda r:r["round"])
            assert [r["round"] for r in runs] == [0,1,2], mode
            assert all(r["validated"] and r["returncodes"] == [0,0] for r in runs)
            assert all(r["client"]["errors"] == 0 for r in runs)
            for r in runs:
                assert len(r["ready"]["pids"]) == (3 if mode.startswith("multiprocess") else 1)
                for thread in r["threads"].values():
                    cpus = set()
                    for part in thread["affinity"].split(","):
                        bounds = list(map(int, part.split("-")))
                        cpus.update(range(bounds[0], bounds[-1]+1))
                    assert cpus <= set(r["server_cpus"]), thread
            rps = [r["client"]["rps"] for r in runs]
            p99 = [r["client"]["p99_us"]/1000 for r in runs]
            lag = [max((w["loop_lag"]["p99_us"]/1000 for w in r["server"]["workers"]
                       if "loop_lag" in w), default=0) for r in runs]
            output[mode] = dict(rps=rps, median_rps=st.median(rps), min_rps=min(rps), max_rps=max(rps),
                p99_ms=p99, median_p99_ms=st.median(p99), median_loop_lag_p99_ms=st.median(lag),
                median_server_cpu_cores=st.median(r["server_cpu_cores"] for r in runs),
                median_cpu_us_per_request=st.median(r["server_cpu_cores"]/r["client"]["rps"]*1e6 for r in runs),
                median_pss_mib=st.median(r["max_sampled_pss_mib"] for r in runs),
                max_client_cpu_cores=max(r["client"]["cpu_cores"] for r in runs),
                throttled_usec=[r["cgroup_delta"]["throttled_usec"] for r in runs],
                native_policy_decisions=[sum(w["delta"].get("native_policy_decisions",0) for w in r["server"]["workers"]) for r in runs],
                python_decisions=[sum(w["delta"].get("python_decisions",0) for w in r["server"]["workers"]) for r in runs])
            report["validated_responses"] += sum(r["client"]["requests"] for r in runs)
            report["validated_runs"] += len(runs)
        def ratio(a,b): return output[a]["median_rps"]/output[b]["median_rps"]
        if workload == "file-shared":
            comparisons = dict(shared_over_native=ratio("seastar-shared","native"),
                shared_over_asyncio_multiprocess=ratio("seastar-shared","multiprocess-asyncio"),
                shared_over_two_reactors=ratio("seastar-shared","seastar-coalesced"),
                paired_shared_over_native=[a/b for a,b in zip(output["seastar-shared"]["rps"],output["native"]["rps"])])
            report["workloads"][workload] = dict(modes=output, comparisons=comparisons)
            continue
        comparisons = dict(policy_over_native=ratio("seastar-policy","native"),
            policy_over_multiprocess=ratio("seastar-policy","multiprocess"),
            dynamic_over_native=ratio("seastar-batch","native"),
            dynamic_over_multiprocess=ratio("seastar-batch","multiprocess"),
            paired_policy_over_native=[a/b for a,b in zip(output["seastar-policy"]["rps"],output["native"]["rps"])])
        if workload == "fixed":
            comparisons |= dict(policy_three_over_one=ratio("seastar-policy","seastar-policy1"),
                native_three_over_one=ratio("native","native1"), policy_over_single=ratio("seastar-policy","single"))
        else:
            comparisons["policy_over_asyncio_multiprocess"] = ratio("seastar-policy","multiprocess-asyncio")
        report["workloads"][workload] = dict(modes=output, comparisons=comparisons)
    (ROOT/"results/final-summary.json").write_text(json.dumps(report,indent=2)+"\n")
    print(json.dumps(report,indent=2))


if __name__ == "__main__": summarize()
