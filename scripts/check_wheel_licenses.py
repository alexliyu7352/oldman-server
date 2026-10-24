"""Check that a release wheel carries the license of all the code it contains.

auditwheel copies into oldman_server.libs/ whatever the extension and the
runtime's io_uring build need at run time beyond what manylinux lets a wheel
expect, which depends on the build image as well as on our build (a library
the image links in by default, for example). Every library there must be one
listed below, whose license file is in the wheel's licenses (LICENSES/ as the
package's license files). None is expected: pybind11 and Asio (header-only)
are compiled into the extension, and liburing into the io_uring build. A
library that is not listed fails the check, so that a new one is reviewed
(and its license added) before a wheel carrying it is released.

    python scripts/check_wheel_licenses.py WHEEL...
"""
import argparse
from pathlib import Path
import re
import sys
import zipfile

# Library name in oldman_server.libs (before auditwheel's hash) -> its license file.
LIBRARIES = {}
# Compiled into the extension (and liburing into its io_uring build), not a
# separate library.
EMBEDDED = ["pybind11-BSD-3-Clause.txt", "asio-BSL-1.0.txt", "liburing-MIT.txt"]


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
