# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
def test_import_and_version():
    import raincloud
    assert isinstance(raincloud.__version__, str)
    assert raincloud.__version__  # non-empty
    # top-level re-exports are present and wired to the hierarchy
    from raincloud import RaincloudError, UnknownSlug
    assert issubclass(UnknownSlug, RaincloudError)


def test_exceptions_hierarchy():
    from raincloud.exceptions import (
        ArtifactNotFound,
        BuildToolingMissing,
        ChecksumMismatch,
        FormatUnavailable,
        MissingDependency,
        OfflineMiss,
        RaincloudError,
        UnknownSlug,
    )
    for exc in (UnknownSlug, FormatUnavailable, ArtifactNotFound, ChecksumMismatch,
                BuildToolingMissing, OfflineMiss, MissingDependency):
        assert issubclass(exc, RaincloudError)
