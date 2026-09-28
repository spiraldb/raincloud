# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Reader availability without importing optional native extensions."""
from importlib.util import find_spec

from .exceptions import MissingDependency


def reader_capabilities() -> dict:
    """Which format readers this install has, per format; imports and fetches nothing."""
    return {
        "arrow": {"available": True, "implementation": "pyarrow"},
        "parquet": {"available": True, "implementation": "pyarrow"},
        "vortex": {"available": find_spec("vortex") is not None, "implementation": "vortex-python", "extra": "vortex"},
    }


def require_reader(fmt: str, *, import_native: bool = True):
    if fmt == "vortex":
        if not reader_capabilities()["vortex"]["available"]:
            raise MissingDependency("Vortex reads require raincloud[vortex] and a supported native wheel for this platform")
        if not import_native:
            return
        try:
            import vortex  # noqa: F401
        except (ImportError, OSError) as exc:
            raise MissingDependency("Vortex reads require raincloud[vortex] and a supported native wheel for this platform") from exc
