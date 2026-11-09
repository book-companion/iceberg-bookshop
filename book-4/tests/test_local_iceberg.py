from pathlib import Path

import pytest

from iceberg_migration.common import Refused
from iceberg_migration.fixture import rows,create_table,json_rows,revenue
from iceberg_migration.probes import pilot,raw
from iceberg_migration.parquet import seed_zone,normalize


def test_pilot_uses_actual_iceberg_and_consumer_queries(tmp_path):
    result=pilot(tmp_path)
    assert result['status']=='passed' and result['consumer_cases']==4
    assert (tmp_path/'catalog.sqlite').is_file()
    assert (tmp_path/'baseline.json').is_file() and (tmp_path/'reconciliation.json').is_file()


def test_quarantine_keeps_source_and_blocks_whole_release(tmp_path):
    result=raw(tmp_path)
    assert result['normalization']['accepted_rows']==8
    assert result['normalization']['quarantined_files']==2
    assert result['normalization']['release_eligible'] is False
    assert len(list((tmp_path/'zone').glob('*.parquet')))==4
    with pytest.raises(Refused):normalize(tmp_path/'zone',tmp_path/'normalized')


def test_snapshot_isolation_during_append(tmp_path):
    table=create_table(tmp_path,rows());snapshot=table.current_snapshot().snapshot_id
    from iceberg_migration.fixture import arrow
    extra={**rows()[0],'order_id':41999}
    table.append(arrow([extra]))
    old=json_rows(table.scan(snapshot_id=snapshot).to_arrow())
    assert len(old)==8 and revenue(old)==39000
    assert len(json_rows(table.scan().to_arrow()))==9
