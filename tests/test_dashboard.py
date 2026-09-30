from pathlib import Path
import sys
from types import SimpleNamespace
import pandas as pd
from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'streamlit_app'))
from components import data_utils


def test_pagination_includes_records_after_server_limit():
    class Query:
        def range(self, start, end):
            self.start = start
            return self
        def execute(self):
            pages = {0: [{'season': 2026}, {'season': 2026}], 2: [{'season': 2025}]}
            return SimpleNamespace(data=pages[self.start])
    assert data_utils._read_pages(Query(), page_size=2).season.tolist() == [2026, 2026, 2025]


def test_all_dashboard_pages_select_latest_loaded_season(monkeypatch):
    from components import season_picker
    from views import home, by_position, team, player
    record = dict(
        season=2026, player_name='Test Player', gsis_id='p1', team='BUF', position='QB',
        yearly_cap_hit=10., total_epa=12., passing_epa=10., rushing_epa=2., receiving_epa=0.,
        snaps=150, epa_per_snap=.08, cost_per_epa=10/12, sample_flag='ok',
        is_rookie_deal=False, notes='Regular season through week 3; 48 shared games. APY in millions.',
    )
    frame = pd.DataFrame([record])
    monkeypatch.setattr(season_picker, 'load_available_seasons', lambda: [2026, 2025])
    meta = pd.DataFrame([dict(last_run=pd.Timestamp.now(tz='UTC').isoformat(), last_status='success')])
    monkeypatch.setattr(data_utils, 'load_pipeline_meta', lambda: meta)
    monkeypatch.setattr(home, 'load_pipeline_meta', lambda: meta)
    for module in [home, by_position, team, player]:
        monkeypatch.setattr(module, 'load_offense_roster', lambda season: frame.copy())
    monkeypatch.setattr(team, 'load_team_efficiency', lambda season: pd.DataFrame([dict(team='BUF', team_total_epa=12., team_total_cap_dollars=10_000_000.)]))
    monkeypatch.setattr(player, 'load_player_history', lambda gsis_id: frame.copy())
    app = AppTest.from_file(str(ROOT / 'streamlit_app' / 'app.py')).run()
    for name in ['Home', 'By Position', 'Team Efficiency', 'Player Detail', 'About / Methodology']:
        app.sidebar.radio[0].set_value(name).run()
        assert not app.exception, (name, app.exception)
        if name != 'About / Methodology':
            assert app.selectbox[0].value == 2026
