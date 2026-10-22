"""Offline export, strict accounting and lossless archiving of a single capture."""
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

from probes.application_lifecycle.inputs import fingerprint
from probes.blocking_attribution.analyze import event_stats, parse, summarize, summarize_threads
from probes.blocking_attribution.record import perf_binary


def export(out, tid, begin_ns, end_ns, *, capture_row=None):
    out = Path(out)
    source = fingerprint(out / 'perf.data')
    if capture_row is not None:
        if source['sha256'] != capture_row['perf_data']['sha256']:
            raise ValueError('capture hash differs from validated row')
        window = capture_row['host_window']
        if (capture_row['ready']['pids'] != [tid] or
                (begin_ns, end_ns) != (window['begin_sent_ns'], window['end_received_ns']) or
                capture_row.get('profiler') not in ('switches', 'switches-shards') or
                not capture_row.get('diagnostic_only') or not capture_row.get('experiment_validated')):
            raise ValueError('capture identity/window differs from validated row')
    perf = perf_binary()
    tool = fingerprint(perf)
    captured_tool = (capture_row or {}).get('perf_control', {}).get('perf_executable')
    if captured_tool is not None and captured_tool != tool:
        raise ValueError('perf executable differs from capture')
    commands = [
        [perf, "report", "--stdio", "--stats", "--no-children", "--sort", "comm", "--no-inline", "-i", str(out / "perf.data")],
        [perf, "script", "--no-inline", "--ns", "--show-switch-events", "--show-lost-events",
         "-F", "comm,pid,tid,time,event,ip,sym,symoff,dso", "-i", str(out / "perf.data")]]
    for name in ("event-stats", "script", "export-commands", "attribution", "archives"):
        if any((out / (name + suffix)).exists() for suffix in (".txt", ".txt.gz", ".json")):
            raise FileExistsError(out / name)
    with (out / "export-commands.json").open("x") as stream:
        json.dump(commands, stream, indent=2)
        stream.write("\n")
    for name, command in zip(("event-stats", "script"), commands, strict=True):
        with (out / (name + ".txt")).open("x") as stdout, (out / (name + ".log")).open("x") as stderr:
            subprocess.run(command, stdout=stdout, stderr=stderr, check=True)
    stats = event_stats((out / "event-stats.txt").read_text())
    with (out / "script.txt").open() as stream:
        events = parse(stream)
    if capture_row is not None and capture_row['profiler'] == 'switches-shards':
        tids = [tid, *(s['tid'] for s in capture_row['ready']['workers'][0]['shards'])]
        if capture_row['perf_control'].get('tids') != tids:
            raise ValueError('recorded target threads differ from ready reactors')
        result = summarize_threads(events, tid, tids, begin_ns, end_ns)
    else:
        result = summarize(events, tid, begin_ns, end_ns)
    if stats["SAMPLE"] != result["all_samples"] or stats["SWITCH"] != result["all_out"] + result["all_in"]:
        raise ValueError("perf event/script accounting differs")
    result["perf_event_stats"] = stats
    result['export_provenance'] = {'perf': tool,
        'sources': [fingerprint(p) for p in sorted(Path(__file__).parent.glob('*.py'))],
        'fingerprint_source': fingerprint(fingerprint.__code__.co_filename),
        'validated_capture_row': capture_row is not None,
        'capture_row_sha256': hashlib.sha256(json.dumps(capture_row, sort_keys=True).encode()).hexdigest()
            if capture_row is not None else None}
    result["inputs"] = [fingerprint(out / name) for name in ("perf.data", "script.txt", "event-stats.txt")]
    with (out / "attribution.json").open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    archives = []
    for name in ("perf.data", "script.txt"):
        path = out / name
        original = fingerprint(path)
        compressed = path.with_suffix(path.suffix + ".gz")
        with path.open("rb") as src, compressed.open("xb") as dst:
            with gzip.GzipFile(filename="", mode="wb", fileobj=dst, mtime=0) as encoded:
                shutil.copyfileobj(src, encoded)
        digest = hashlib.sha256()
        with gzip.open(compressed, "rb") as decoded:
            for chunk in iter(lambda: decoded.read(1024*1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != original["sha256"]:
            raise ValueError("lossless archive verification failed")
        archives.append({"original": original, "gzip": fingerprint(compressed), "original_bytes": path.stat().st_size})
        path.unlink()  # Only our verified, losslessly archived capture/export.
    with (out / "archives.json").open("x") as stream:
        json.dump(archives, stream, indent=2)
        stream.write("\n")
    return result
