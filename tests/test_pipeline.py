from types import SimpleNamespace
from unittest.mock import MagicMock
import pandas as pd
import pytest
from etl.database import upsert_supabase, check_connection
from etl.etl import merge_local_history, parse_args


def test_database_errors_are_not_swallowed():
    client = MagicMock()
    client.table.return_value.upsert.return_value.execute.return_value = SimpleNamespace(status_code=500)
    with pytest.raises(RuntimeError, match='status 500'):
        upsert_supabase(client, pd.DataFrame([dict(season=2026, gsis_id='p1')]))


def test_upsert_serializes_nulls_and_is_idempotent():
    client = MagicMock()
    client.table.return_value.upsert.return_value.execute.return_value = SimpleNamespace(status_code=200)
    frame = pd.DataFrame(dict(season=[2026, 2026], gsis_id=['p1', 'p2'], age=[float('nan'), 25.]))
    assert upsert_supabase(client, frame, batch_size=1) == 2
    calls = client.table.return_value.upsert.call_args_list
    assert calls[0].args[0][0]['age'] is None
    assert calls[0].kwargs['on_conflict'] == 'season,gsis_id'
    assert len(calls) == 2


def test_healthcheck_rejects_empty_or_inaccessible_metadata():
    client = MagicMock()
    client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = SimpleNamespace(data=[])
    with pytest.raises(RuntimeError, match='missing or inaccessible'):
        check_connection(client)


def test_incremental_artifacts_retain_history(tmp_path):
    path = tmp_path / 'history.csv'
    pd.DataFrame(dict(season=[2025, 2026], gsis_id=['p1', 'p1'], total_epa=[20, 1])).to_csv(path, index=False)
    updated = pd.DataFrame(dict(season=[2026], gsis_id=['p1'], total_epa=[3]))
    frame = merge_local_history(updated, path)
    assert frame.season.tolist() == [2025, 2026]
    assert frame.total_epa.tolist() == [20, 3]


def test_cli_rejects_invalid_modes_and_strength():
    with pytest.raises(SystemExit):
        parse_args(['--auto', '--seasons', '2026'])
    with pytest.raises(SystemExit):
        parse_args(['--shrink-tau', '-1'])
