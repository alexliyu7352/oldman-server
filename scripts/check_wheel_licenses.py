"""Check that a release wheel carries the license of all the code it contains.

auditwheel copies into oldman_server.libs/ whatever the extension and libseastar
need at run time, which depends on the build image as well as on our build
(a library the image links in by default, for example). Every library there
must be one listed below, whose license file is in the wheel's licenses
(LICENSES/ as the package's license files); pybind11 is compiled into the
extension itself. A library that is not listed fails the check, so that a new
one is reviewed (and its license added) before a wheel carrying it is released.

    python scripts/check_wheel_licenses.py WHEEL...
"""
import argparse
from pathlib import Path
import re
import sys
import zipfile

# Library name in oldman_server.libs (before auditwheel's hash) -> its license file.
LIBRARIES = {
    "libseastar": "seastar-Apache-2.0.txt",
    "libboost_program_options": "boost-BSL-1.0.txt",
    "libcares": "c-ares-MIT.txt",
    "libcrypto": "openssl-Apache-2.0.txt",
    "libssl": "openssl-Apache-2.0.txt",
    "libfmt": "fmt-MIT.txt",
    "libhwloc": "hwloc-BSD-3-Clause.txt",
    "liblz4": "lz4-BSD-2-Clause.txt",
    "libprotobuf": "protobuf-BSD-3-Clause.txt",
    "liburing": "liburing-MIT.txt",
    "libyaml-cpp": "yaml-cpp-MIT.txt",
}
# Compiled into the extension, not a separate library.
EMBEDDED = ["pybind11-BSD-3-Clause.txt"]


def problems(wheel):
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
    licenses = {Path(name).name for name in names if "/licenses/" in name and ".dist-info/" in name}
    found = []
    for name in names:
        if name.startswith("oldman_server.libs/") and not name.endswith("/"):
            library = re.sub(r"-[0-9a-f]{8}\.so.*$", "", Path(name).name)
            if library not in LIBRARIES:
                found.append(f"{name}: not a reviewed library (add it and its license)")
            elif LIBRARIES[library] not in licenses:
                found.append(f"{name}: license {LIBRARIES[library]} missing")
    found.extend(f"license {license} missing" for license in EMBEDDED if license not in licenses)
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("wheels", nargs="+", type=Path)
    args = parser.parse_args()
    failed = False
    for wheel in args.wheels:
        for problem in problems(wheel):
            print(f"{wheel.name}: {problem}", file=sys.stderr)
            failed = True
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
