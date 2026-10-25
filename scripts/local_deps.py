"""Extract the immutable dependency lock locally; never run package scripts.

This reproduces the recorded Ubuntu ABI/build dependency set. Incompatible
system versions are rejected explicitly, not silently substituted. Archives
are always extracted, including when their package is also installed globally.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import urllib.request
import zipfile


def installed_packages():
    result = {}
    for block in Path("/var/lib/dpkg/status").read_text().split("\n\n"):
        fields = {}
        for line in block.splitlines():
            if line and not line[0].isspace() and ": " in line:
                key, value = line.split(": ", 1)
                fields[key] = value
        if fields.get("Status") == "install ok installed":
            result[fields["Package"]] = fields["Version"]
    return result


def check_host(lock):
    if lock.get("schema") != 1:
        raise RuntimeError("unsupported dependency lock schema")
    installed = installed_packages()
    mismatches = {name: {"expected": version, "actual": installed.get(name)}
                  for name, version in lock["host_requirements"].items()
                  if installed.get(name) != version}
    if mismatches:
        raise RuntimeError("host version mismatch: " + json.dumps(mismatches, sort_keys=True))
    return installed


def install(root, lock_path, only=None):
    lock_bytes = lock_path.read_bytes()
    lock = json.loads(lock_bytes)
    installed = check_host(lock)
    cache = root / "debs"
    prefix = root / "sysroot"
    cache.mkdir(parents=True, exist_ok=True)
    base = lock.get("archive_base_url", "https://archive.ubuntu.com/ubuntu/")
    entries = [dict(package, Format="deb") for package in lock["packages"]]
    entries.extend(lock.get("archives", []))
    if only is not None:
        entries = [entry for entry in entries if entry.get("Destination", "sysroot") == only]
        if not entries:
            raise RuntimeError("no locked entries for destination " + only)

    def destination_of(entry):
        # --only: that destination's entries go into ROOT itself.
        return root if only is not None else root / entry.get("Destination", "sysroot")

    patch_data = {}
    for entry in entries:
        destination = destination_of(entry).resolve()
        if destination != root.resolve() and root.resolve() not in destination.parents:
            raise RuntimeError("dependency destination must be inside the project dependency directory")
        if entry["Format"] not in ("deb", "tar", "conda"):
            raise RuntimeError("unsupported archive format: " + entry["Format"])
        for patch in entry.get("Patches", []):
            path = (lock_path.parent / patch["File"]).resolve()
            if not path.is_relative_to(lock_path.parent.resolve()):
                raise RuntimeError("patch must be inside the lock directory")
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != patch["SHA256"]:
                raise RuntimeError("patch checksum mismatch: " + patch["File"])
            patch_data[patch["File"]] = data

    def fetch(package):
        archive_cache = cache if package["Format"] == "deb" else root / "downloads"
        archive_cache.mkdir(exist_ok=True)
        target = archive_cache / Path(package["Filename"]).name
        name = package.get("Package", package.get("Name"))
        if not target.exists():
            for attempt in range(5):
                try:
                    url = package.get("URL", base + package["Filename"])
                    with urllib.request.urlopen(url, timeout=90) as response:
                        data = response.read()
                    if hashlib.sha256(data).hexdigest() != package["SHA256"]:
                        raise RuntimeError("checksum mismatch: " + name)
                    target.write_bytes(data)
                    break
                except Exception:
                    if attempt == 4:
                        raise
                    time.sleep(1)
        if hashlib.sha256(target.read_bytes()).hexdigest() != package["SHA256"]:
            raise RuntimeError("checksum mismatch: " + name)
        return target

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        archives = list(pool.map(fetch, entries))
    for entry, archive in zip(entries, archives):
        destination = destination_of(entry)
        destination.mkdir(parents=True, exist_ok=True)
        if entry["Format"] == "deb":
            subprocess.run(["dpkg-deb", "-x", str(archive), str(destination)], check=True)
        elif entry["Format"] == "tar":
            subprocess.run(["tar", "-xf", str(archive), "-C", str(destination),
                            "--strip-components=" + str(entry.get("StripComponents", 0))], check=True)
        else:
            # Only unpack the two selected runtime payloads; no conda environment,
            # solver, activation scripts, or package install hooks are executed.
            with zipfile.ZipFile(archive) as package, tempfile.TemporaryDirectory(dir=cache) as temp:
                payloads = [name for name in package.namelist()
                            if name.startswith("pkg-") and name.endswith(".tar.zst")]
                if len(payloads) != 1:
                    raise RuntimeError("expected one conda package payload: " + str(archive))
                payload = Path(temp) / "payload.tar.zst"
                with package.open(payloads[0]) as source, payload.open("wb") as target:
                    shutil.copyfileobj(source, target)
                subprocess.run(["tar", "--zstd", "-xf", str(payload), "-C", str(destination)], check=True)
        for patch in entry.get("Patches", []):
            data = patch_data[patch["File"]]
            subprocess.run(["git", "apply", "--check", "-"], input=data, cwd=destination, check=True)
            subprocess.run(["git", "apply", "-"], input=data, cwd=destination, check=True)
    # A dev package may link to an already installed, version-checked runtime.
    # Include nested GCC paths to avoid accidentally selecting static libstdc++.
    for link in (prefix / "usr/lib").rglob("*.so"):
        if link.is_symlink() and not link.exists():
            destination = Path(os.path.normpath(link.parent / os.readlink(link)))
            system = Path("/usr/lib/x86_64-linux-gnu") / destination.name
            if system.exists() and not destination.exists():
                destination.symlink_to(system)
    observed = {"lock_sha256": hashlib.sha256(lock_bytes).hexdigest(),
                "destination": only,
                "extracted": [entry for entry in lock["packages"]
                              if only is None or entry.get("Destination", "sysroot") == only],
                "archives": [entry for entry in entries if entry["Format"] != "deb"],
                "host_versions": {name: installed[name] for name in lock["host_requirements"]}}
    (root / "install-observed.json").write_text(json.dumps(observed, indent=2) + "\n")
    print(f"Extracted {len(archives)} locked archives into {root}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--lock", type=Path,
                        default=Path(__file__).resolve().parents[1] / "dependency-lock.json")
    parser.add_argument("--only", metavar="DESTINATION",
                        help="Extract only the entries of this destination, into ROOT itself "
                             "(the GCC 14 toolchain: ROOT --only toolchain)")
    parser.add_argument("--check", action="store_true",
                        help="Check host compatibility without downloading or extracting packages")
    args = parser.parse_args()
    if args.check:
        check_host(json.loads(args.lock.read_bytes()))
        print("Host dependency requirements satisfied")
    else:
        install(args.root.absolute(), args.lock.absolute(), args.only)


if __name__ == "__main__":
    main()
