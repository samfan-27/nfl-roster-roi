import numpy as np
import pandas as pd
from unittest.mock import MagicMock
from src.valuation import score_completed_seasons


def test_partial_season_never_enters_training_or_scoring(monkeypatch):
    import src.valuation as valuation
    rows = []
    for season in [2025, 2026]:
        for player in range(6):
            rows.append(dict(season=season, gsis_id=f'p{player}', position='WR', total_epa=10., snaps=100, epa_per_snap=.1, age=25, years_exp=4, is_rookie_deal=False, yearly_cap_hit=5., cap_pct_of_team=.01))
    seen = []
    def search(frame, groups):
        seen.extend(frame.season.tolist())
        model = MagicMock()
        model.best_params_ = {'ridge__alpha': 1.}
        model.predict.side_effect = lambda X: np.full(len(X), np.log1p(5 / 279.2 * 100))
        return model
    monkeypatch.setattr(valuation, '_search', search)
    scored, diagnostics = score_completed_seasons(pd.DataFrame(rows), incomplete_season=2026)
    assert set(seen) == {2025}
    assert set(scored.season) == {2025}
    assert diagnostics.veteran_players.iloc[0] == 6
    assert diagnostics.grouped_oof_mae_m.iloc[0] < 1e-10
