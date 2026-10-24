"""Download, check and unpack the source archives the release build compiles.

dependency-lock.json pins them: its "archives" of the tar format (Seastar with
its patch, Boost, OpenSSL, fmt, yaml-cpp, c-ares, liburing, Asio) and its
"release_archives" (hwloc, protobuf, ragel; the development host uses its own
packages of these). Each archive must match its SHA-256; it is unpacked into
DIRECTORY/<Name> (the lock's StripComponents applied) and its patches applied.
An archive already unpacked from the same checksums (archive and patches) is
left alone.

    python scripts/fetch_sources.py DIRECTORY
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def entries(lock):
    archives = [entry for entry in lock["archives"] if entry["Format"] == "tar"]
    return archives + lock["release_archives"]


def download(entry, cache):
    target = cache / entry["Filename"]
    if not target.exists() or hashlib.sha256(target.read_bytes()).hexdigest() != entry["SHA256"]:
        for attempt in range(5):
            try:
                with urllib.request.urlopen(entry["URL"], timeout=120) as response:
                    data = response.read()
                break
            except OSError:
                if attempt == 4:
                    raise
                time.sleep(2 * (attempt + 1))
        if hashlib.sha256(data).hexdigest() != entry["SHA256"]:
            raise RuntimeError(f"checksum mismatch: {entry['Name']} from {entry['URL']}")
        target.write_bytes(data)
    return target


def unpack(entry, archive, directory):
    destination = directory / entry["Name"]
    marker = destination / ".source-sha256"
    # A changed patch needs a fresh unpack too: patches apply only once.
    checksums = " ".join([entry["SHA256"], *(patch["SHA256"] for patch in entry.get("Patches", []))])
    if marker.exists() and marker.read_text() == checksums:
        return
    shutil.rmtree(destination, ignore_errors=True)
    destination.mkdir(parents=True)
    subprocess.run(["tar", "-xf", str(archive), "-C", str(destination),
                    f"--strip-components={entry.get('StripComponents', 0)}"], check=True)
    for patch in entry.get("Patches", []):
        data = (ROOT / patch["File"]).read_bytes()
        if hashlib.sha256(data).hexdigest() != patch["SHA256"]:
            raise RuntimeError(f"patch checksum mismatch: {patch['File']}")
        subprocess.run(["git", "apply", "-"], input=data, cwd=destination, check=True)
    marker.write_text(checksums)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    lock = json.loads((ROOT / "dependency-lock.json").read_text())
    cache = args.directory / "downloads"
    cache.mkdir(parents=True, exist_ok=True)
    for entry in entries(lock):
        unpack(entry, download(entry, cache), args.directory)
        print(f"{entry['Name']} {entry['Version']}", flush=True)


if __name__ == "__main__":
    main()
