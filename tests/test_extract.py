import json
from datetime import date

from ingestion.extract import extract_day, partition_dir

DT = date(2026, 11, 2)
ROLES = ["data engineer", "data analyst"]


def test_writes_one_file_per_page_in_date_partition(tmp_path, fake_source):
    stats = extract_day(fake_source, DT, tmp_path, max_pages=5, roles=ROLES)
    folder = tmp_path / "dt=2026-11-02"
    assert folder == partition_dir(tmp_path, DT)
    assert sorted(p.name for p in folder.iterdir()) == [
        "data_analyst_page_001.json",
        "data_engineer_page_001.json",
        "data_engineer_page_002.json",
    ]
    assert stats == {"files": 3, "postings": 5}


def test_envelope_keeps_raw_response_and_metadata(tmp_path, fake_source):
    extract_day(fake_source, DT, tmp_path, max_pages=5, roles=ROLES)
    env = json.loads((tmp_path / "dt=2026-11-02" / "data_engineer_page_001.json").read_text())
    assert env["snapshot_date"] == "2026-11-02"
    assert env["search_role"] == "data engineer"
    assert env["page"] == 1
    assert [r["id"] for r in env["response"]["results"]] == ["1", "2"]


def test_stops_paging_on_short_page_and_respects_max_pages(tmp_path, fake_source):
    extract_day(fake_source, DT, tmp_path, max_pages=5, roles=ROLES)
    # page 2 of data engineer was short, so page 3 is never requested
    assert ("data engineer", 3) not in fake_source.calls
    fake_source.calls.clear()
    extract_day(fake_source, DT, tmp_path, max_pages=1, roles=ROLES)
    assert all(page == 1 for _, page in fake_source.calls)


def test_rerun_replaces_partition_instead_of_appending(tmp_path, fake_source):
    extract_day(fake_source, DT, tmp_path, max_pages=5, roles=ROLES)
    stale = tmp_path / "dt=2026-11-02" / "stale_file.json"
    stale.write_text("{}")
    extract_day(fake_source, DT, tmp_path, max_pages=5, roles=ROLES)
    assert not stale.exists()
    assert len(list((tmp_path / "dt=2026-11-02").iterdir())) == 3
    assert not (tmp_path / "dt=2026-11-02.tmp").exists()


def test_failed_run_leaves_previous_partition_untouched(tmp_path, fake_source):
    extract_day(fake_source, DT, tmp_path, max_pages=5, roles=ROLES)

    class Broken:
        results_per_page = 2

        def search(self, role, page):
            raise RuntimeError("API down")

    try:
        extract_day(Broken(), DT, tmp_path, max_pages=5, roles=ROLES)
    except RuntimeError:
        pass
    assert len(list((tmp_path / "dt=2026-11-02").iterdir())) == 3
