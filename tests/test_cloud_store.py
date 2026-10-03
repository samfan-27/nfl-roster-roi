from contextlib import contextmanager
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import MagicMock
import pandas as pd
import pytest
from etl.cloud_store import CloudStore,PipelineBusy,SchemaNotReady,METHODOLOGY_COLUMNS,read_pages
from postgrest.exceptions import APIError
from etl import cloud


def test_service_pagination_uses_every_stably_ordered_page():
    query=MagicMock()
    query.range.return_value.execute.side_effect=[SimpleNamespace(data=[{'id':1},{'id':2}]),SimpleNamespace(data=[{'id':3}])]
    assert read_pages(query,page_size=2)==[{'id':1},{'id':2},{'id':3}]
    assert query.range.call_args_list[1].args==(2,3)


def test_concurrent_attempt_is_rejected_and_never_releases_another_owner():
    client=MagicMock();client.rpc.return_value.execute.return_value.data=False
    store=CloudStore(client)
    with pytest.raises(PipelineBusy):
        with store.lock():pass
    assert client.rpc.call_count==1


def test_upload_or_readback_failure_prevents_artifact_acceptance():
    client=MagicMock()
    client.table.return_value.select.return_value.eq.return_value.execute.return_value.data=[]
    client.rpc.return_value.execute.return_value.data=0
    store=CloudStore(client);store.owner='owner'
    store.bucket.upload.side_effect=RuntimeError('upload failed')
    with pytest.raises(RuntimeError,match='upload failed'):store.put(b'test','md','text/markdown')
    store.bucket.upload.side_effect=None
    store.bucket.download.return_value=b'corrupt'
    with pytest.raises(RuntimeError,match='hash mismatch'):store.put(b'test','md','text/markdown')


def test_budget_stops_upload_before_free_storage_exhaustion():
    client=MagicMock()
    client.table.return_value.select.return_value.eq.return_value.execute.return_value.data=[]
    client.rpc.return_value.execute.side_effect=[SimpleNamespace(data=None),SimpleNamespace(data=9)]
    store=CloudStore(client,storage_limit=10);store.owner='owner'
    with pytest.raises(RuntimeError,match='budget'):store.put(b'test','md','text/markdown')
    store.bucket.upload.assert_not_called()


def test_cloud_input_hashes_and_history_are_required():
    client=MagicMock();store=CloudStore(client)
    store.bucket.download.return_value=b'corrupt'
    with pytest.raises(RuntimeError,match='input hash'):store.get(dict(path='input',bytes=4,sha256=sha256(b'test').hexdigest()))
    client.table.return_value.select.return_value.lt.return_value.order.return_value.range.return_value.execute.return_value.data=[]
    with pytest.raises(ValueError,match='history'):store.histories(2026)


def test_execution_retries_have_stable_run_id(monkeypatch):
    monkeypatch.setenv('CLOUD_RUN_EXECUTION','job-execution')
    monkeypatch.setenv('CLOUD_RUN_TASK_ATTEMPT','0')
    first=cloud.get_run_id('weekly',2026)
    monkeypatch.setenv('CLOUD_RUN_TASK_ATTEMPT','1')
    assert cloud.get_run_id('weekly',2026)==first
    assert cloud.get_run_id('refresh',2026)!=first


def test_failed_required_write_never_publishes_and_records_only_error_type(monkeypatch):
    store=MagicMock();store.lock.return_value.__enter__.return_value=store
    store.begin.return_value=True;store.previous_games.return_value=[]
    monkeypatch.setattr(cloud,'configure_sources',lambda **kw:None)
    monkeypatch.setattr(cloud,'load_reference_tables',lambda: {})
    monkeypatch.setattr(cloud,'load_season_tables',lambda s: {name:pd.DataFrame({'game_id':[]}) for name in ('player_stats','snap_counts')})
    monkeypatch.setattr(cloud,'load_schedule',lambda s:None)
    coverage=dict(cutoff_week=1,shared_games=['g1'],overdue_games=[],grace_games=[])
    monkeypatch.setattr(cloud,'assess_coverage',lambda *a,**kw:coverage)
    monkeypatch.setattr(cloud,'through_week',lambda tables,week:tables)
    monkeypatch.setattr(cloud,'archive_inputs',lambda *a,**kw: (_ for _ in ()).throw(RuntimeError('secret in upstream transport')))
    with pytest.raises(RuntimeError):cloud.execute(store,'weekly',2026,code_revision='test')
    store.publish.assert_not_called()
    assert store.finish.call_args.args== (store.begin.call_args.args[0],'failed',coverage,'RuntimeError')


def test_successful_retry_does_not_download_or_republish(monkeypatch):
    store=MagicMock();store.begin.return_value=False
    loader=MagicMock();monkeypatch.setattr(cloud,'load_reference_tables',loader)
    assert cloud.execute(store,'weekly',2026,code_revision='test')['status']=='already_succeeded'
    loader.assert_not_called();store.publish.assert_not_called()


def test_missing_release_is_a_visible_deferral_without_publication(monkeypatch):
    from etl.sources import SourceNotReadyError
    store=MagicMock();store.begin.return_value=True
    monkeypatch.setattr(cloud,'configure_sources',lambda **kw:None)
    monkeypatch.setattr(cloud,'load_reference_tables',lambda: {})
    monkeypatch.setattr(cloud,'load_season_tables',lambda s: (_ for _ in ()).throw(SourceNotReadyError('not released')))
    result=cloud.execute(store,'weekly',2026,code_revision='test')
    assert result['status']=='deferred'
    store.publish.assert_not_called()
    assert store.finish.call_args.args[1]=='deferred'


def test_staged_batches_are_fenced_and_do_not_commit_after_failure():
    client=MagicMock();store=CloudStore(client);store.owner='owner'
    client.rpc.return_value.execute.side_effect=[SimpleNamespace(data=None),RuntimeError('failed second batch')]
    frame=pd.DataFrame([dict(season=2026,gsis_id=f'p{i}',snaps=100,total_epa=1,yearly_cap_hit=1,position='QB',notes='test') for i in range(201)])
    with pytest.raises(RuntimeError,match='second batch'):store.ingest('run',2026,frame,{}, {})
    assert all(c.args[0]=='stage_pipeline_metrics' for c in client.rpc.call_args_list)
    assert client.rpc.call_args_list[0].args[1]['p_reset'] is True
    assert client.rpc.call_args_list[1].args[1]['p_reset'] is False


def test_regression_baseline_includes_canonical_history_and_dashboard():
    client=MagicMock();store=CloudStore(client)
    queries={name:MagicMock() for name in ('ingestion_heads','history_heads')}
    client.table.side_effect=lambda name:queries[name]
    for name,run_id in [('ingestion_heads','daily'),('history_heads','history')]:
        queries[name].select.return_value.eq.return_value.execute.return_value.data=[{'run_id':run_id}]
    store.snapshot=lambda key:dict(coverage=dict(shared_games=['g1'] if key=='daily' else ['g1','g2']))
    assert store.previous_games(2025)==['g1','g2']


@pytest.mark.parametrize('code', ['42703', 'PGRST204'])
def test_missing_financial_schema_has_safe_migration_guidance(code):
    client=MagicMock();store=CloudStore(client)
    client.table.return_value.select.return_value.limit.return_value.execute.side_effect=APIError(
        dict(code=code,message='transport secret',details=None,hint=None))
    with pytest.raises(SchemaNotReady,match='20261002_apy_methodology.sql') as error:
        store.require_methodology_schema()
    assert 'transport secret' not in str(error.value)
    client.table.return_value.select.assert_called_once_with(','.join(METHODOLOGY_COLUMNS))
    client.table.return_value.select.return_value.limit.assert_called_once_with(0)


def test_schema_check_does_not_mislabel_database_access_failure():
    client=MagicMock();store=CloudStore(client)
    client.table.return_value.select.return_value.limit.return_value.execute.side_effect=APIError(
        dict(code='42501',message='permission denied',details=None,hint=None))
    with pytest.raises(APIError):store.require_methodology_schema()


def test_missing_migration_stops_before_source_reads_or_cloud_archives(monkeypatch):
    store=MagicMock();store.begin.return_value=True
    store.require_methodology_schema.side_effect=SchemaNotReady()
    loader=MagicMock();monkeypatch.setattr(cloud,'load_reference_tables',loader)
    archive=MagicMock();monkeypatch.setattr(cloud,'archive_inputs',archive)
    with pytest.raises(SchemaNotReady):cloud.execute(store,'refresh',2026,code_revision='test')
    loader.assert_not_called();archive.assert_not_called();store.ingest.assert_not_called()
    store.finish.assert_called_once_with(store.begin.call_args.args[0],'failed',None,'SchemaNotReady')


def test_missing_migration_cli_reports_required_action_without_transport_data(monkeypatch,capsys):
    monkeypatch.setattr(cloud,'get_supabase_client',MagicMock())
    monkeypatch.setattr(cloud,'execute',MagicMock(side_effect=SchemaNotReady()))
    assert cloud.main(['refresh','--season','2026'])==1
    import json
    result=json.loads(capsys.readouterr().out)
    assert result['error_type']=='SchemaNotReady'
    assert '20261002_apy_methodology.sql' in result['action']
