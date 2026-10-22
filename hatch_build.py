"""Wheel build hook for the native extension.

The extension (oldman_server.native._native) is built with CMake outside the
Python build: scripts/build.sh during development, scripts/build_wheels.sh for
a release. A wheel built while the building interpreter's extension is in the
tree carries it and is tagged for that interpreter and platform (the release
then makes it a manylinux wheel with auditwheel). Without it the wheel is pure
Python, as an editable install for development is; OLDMAN_SERVER_REQUIRE_NATIVE=1
makes that an error instead, so a release cannot publish one.
"""
import os
import sysconfig
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class NativeExtension(BuildHookInterface):
    def initialize(self, version, build_data):
        if self.target_name != "wheel" or version != "standard":
            return
        name = "_native" + sysconfig.get_config_var("EXT_SUFFIX")
        extension = Path(self.root, "src", "oldman_server", "native", name)
        if not extension.is_file():
            if os.environ.get("OLDMAN_SERVER_REQUIRE_NATIVE") == "1":
                raise RuntimeError(f"the native extension {extension} has not been built")
            return
        # *.so is git-ignored, so only this interpreter's extension goes in.
        build_data["force_include"][str(extension)] = f"oldman_server/native/{name}"
        build_data["pure_python"] = False
        build_data["infer_tag"] = True
