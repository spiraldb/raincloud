import pytest


def test_fetch_copies_bytes(tmp_path):
    from raincloud import _transport
    src = tmp_path / "remote.bin"; src.write_bytes(b"payload")
    dest = tmp_path / "local.part"
    _transport.fetch(f"file://{src}", dest)
    assert dest.read_bytes() == b"payload"


def test_fetch_missing_raises_artifact_not_found(tmp_path):
    from raincloud import _transport
    from raincloud.exceptions import ArtifactNotFound
    dest = tmp_path / "local.part"
    with pytest.raises(ArtifactNotFound):
        _transport.fetch(f"file://{tmp_path}/does-not-exist.bin", dest)
    assert not dest.exists()  # cleanup contract: no partial file left on a miss
