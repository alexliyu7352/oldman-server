"""Download, check and unpack source archives that dependency-lock.json pins.

The release build names the archives it compiles (liburing and Asio). Each
must be one of the lock's "archives" of the tar format and match its SHA-256;
it is unpacked into DIRECTORY/<Name> (the lock's StripComponents applied) and
its patches applied. An archive already unpacked from the same checksums
(archive and patches) is left alone.

    python scripts/fetch_sources.py DIRECTORY NAME...
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


def entries(lock, names):
    archives = {entry["Name"]: entry for entry in lock["archives"] if entry["Format"] == "tar"}
    unknown = [name for name in names if name not in archives]
    if unknown:
        raise SystemExit(f"not a source archive of the lock: {', '.join(unknown)}")
    return [archives[name] for name in names]


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
    parser.add_argument("names", nargs="+", metavar="NAME")
    args = parser.parse_args()
    lock = json.loads((ROOT / "dependency-lock.json").read_text())
    cache = args.directory / "downloads"
    cache.mkdir(parents=True, exist_ok=True)
    for entry in entries(lock, args.names):
        unpack(entry, download(entry, cache), args.directory)
        print(f"{entry['Name']} {entry['Version']}", flush=True)


if __name__ == "__main__":
    main()
