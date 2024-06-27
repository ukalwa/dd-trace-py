#!/usr/bin/env python3
"""
Script to download all required wheels (including dependencies) of the ddtrace
Python package for relevant Python versions (+ abis), C library platforms and
architectures and unpack them into Python-specific site-packages directories.

These site-package directories provide a portable installation of ddtrace which can be
used on multiple platforms and architectures.

Currently, the only OS supported is Linux.

This script has been tested with pip 21.0.0 and is confirmed to not work with
20.0.2.

Usage:
        ./dl_wheels.py --help

"""
import argparse
import itertools
import os
from pathlib import Path
import shutil
import subprocess
import sys

import packaging.version


# Do a check on the pip version since older versions are known to be
# incompatible.
MIN_PIP_VERSION = packaging.version.parse("21.0")
cmd = [sys.executable, "-m", "pip", "--version"]
res = subprocess.run(cmd, capture_output=True)
out = res.stdout.decode().split(" ")[1]
pip_version = packaging.version.parse(out)
if pip_version < MIN_PIP_VERSION:
    print(
        "WARNING: using known incompatible version, %r, of pip. The minimum compatible pip version is %r"
        % (pip_version, MIN_PIP_VERSION)
    )

# Supported Python versions lists all python versions that can install at least one version of the ddtrace library.
supported_versions = ["2.7", "3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"]
supported_arches = ["aarch64", "x86_64", "i686"]
supported_platforms = ["musllinux_1_1", "manylinux2014"]

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--python-version",
    choices=supported_versions,
    action="append",
    required=True,
)
parser.add_argument(
    "--arch",
    choices=supported_arches,
    action="append",
    required=True,
)
parser.add_argument(
    "--platform",
    choices=supported_platforms,
    action="append",
    required=True,
)
parser.add_argument("--input-dir", type=str, required=True)
parser.add_argument("--output-dir", type=str, required=True)
parser.add_argument("--dry-run", action="store_true")
parser.add_argument("--verbose", action="store_true")
args = parser.parse_args()

dl_dir = args.output_dir
print("saving wheels to %s" % dl_dir)

for python_version, platform in itertools.product(args.python_version, args.platform):
    version_filter = "cp%s" % python_version.replace(".", "")

    wheel_files = [f for f in os.listdir(args.input_dir) if f.endswith(".whl") and version_filter in f and platform in f and any([ a in f for a in args.arch]) ]
    for whl in wheel_files:
        wheel_file = os.path.join(args.input_dir, whl)
        print("Unpacking %s" % wheel_file)
        # -q for quieter output, else we get all the files being unzipped.
        subprocess.run(
            [
                "unzip",
                "-q",
                "-o",
                wheel_file,
                "-d",
                os.path.join(dl_dir, "site-packages-ddtrace-py%s-%s" % (python_version, platform)),
            ]
        )

    sitepackages_root = Path(dl_dir) / f"site-packages-ddtrace-py{python_version}-{platform}"
    directories_to_remove = [
        sitepackages_root / "google" / "protobuf",
        sitepackages_root / "google" / "_upb",
    ]
    directories_to_remove.extend(sitepackages_root.glob("protobuf-*"))  # dist-info directories

    for directory in directories_to_remove:
        try:
            shutil.rmtree(directory)
        except Exception:
            pass
