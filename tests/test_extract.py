import json
from datetime import date

from ingestion.extract import extract_day

DT = date(2026, 11, 2)
ROLES = ["data engineer", "data analyst"]


def names(store, dt=DT):
    return [name for name, _ in store.read_partition(dt)]


def test_writes_one_file_per_page_in_date_partition(store, fake_source):
    stats = extract_day(fake_source, DT, store, max_pages=5, roles=ROLES)
    assert "dt=2026-11-02" in store.describe(DT)
    assert names(store) == [
        "data_analyst_page_001.json",
        "data_engineer_page_001.json",
        "data_engineer_page_002.json",
    ]
    assert stats == {"files": 3, "postings": 5}


def test_envelope_keeps_raw_response_and_metadata(store, fake_source):
    extract_day(fake_source, DT, store, max_pages=5, roles=ROLES)
    body = dict(store.read_partition(DT))["data_engineer_page_001.json"]
    env = json.loads(body)
    assert env["snapshot_date"] == "2026-11-02"
    assert env["search_role"] == "data engineer"
    assert env["page"] == 1
    assert [r["id"] for r in env["response"]["results"]] == ["1", "2"]


def test_stops_paging_on_short_page_and_respects_max_pages(store, fake_source):
    extract_day(fake_source, DT, store, max_pages=5, roles=ROLES)
    # page 2 of data engineer was short, so page 3 is never requested
    assert ("data engineer", 3) not in fake_source.calls
    fake_source.calls.clear()
    extract_day(fake_source, DT, store, max_pages=1, roles=ROLES)
    assert all(page == 1 for _, page in fake_source.calls)


def test_rerun_replaces_partition_instead_of_appending(store, fake_source):
    extract_day(fake_source, DT, store, max_pages=5, roles=ROLES)
    # a re-run that returns fewer pages must remove the old extra file
    extract_day(fake_source, DT, store, max_pages=1, roles=ROLES)
    assert names(store) == ["data_analyst_page_001.json", "data_engineer_page_001.json"]


def test_failed_run_leaves_previous_partition_untouched(store, fake_source):
    extract_day(fake_source, DT, store, max_pages=5, roles=ROLES)

    class Broken:
        results_per_page = 2

        def search(self, role, page):
            raise RuntimeError("API down")

    try:
        extract_day(Broken(), DT, store, max_pages=5, roles=ROLES)
    except RuntimeError:
        pass
    assert len(names(store)) == 3


def test_partitions_are_listed_by_date(store, fake_source):
    for d in (date(2026, 11, 3), DT):
        extract_day(fake_source, d, store, max_pages=5, roles=ROLES)
    assert store.list_partitions() == [DT, date(2026, 11, 3)]


def test_migrate_local_days_to_s3(tmp_path, fake_source, monkeypatch):
    import boto3
    from moto import mock_aws

    from ingestion.migrate_local_to_s3 import migrate
    from ingestion.storage import LocalStore, S3Store

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    local = LocalStore(tmp_path)
    for d in (DT, date(2026, 11, 3)):
        extract_day(fake_source, d, local, max_pages=5, roles=ROLES)
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="raw-bucket")
        s3 = S3Store("raw-bucket", client)
        assert migrate(local, s3) == 2
        assert migrate(local, s3) == 2  # re-run is safe
        assert s3.list_partitions() == local.list_partitions()
        assert s3.read_partition(DT) == local.read_partition(DT)
