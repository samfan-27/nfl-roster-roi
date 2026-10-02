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


def test_rookies_hold_out_later_veteran_targets_and_bounds_are_fold_trained(monkeypatch):
    import src.valuation as valuation
    rows=[]
    for i in range(8):
        for year in [2024,2025]:
            rows.append(dict(season=year,gsis_id=f'p{i}',position='WR',total_epa=10.,snaps=100,epa_per_snap=.1,age=25,years_exp=4,
                             is_rookie_deal=(i==0 and year==2024),yearly_cap_hit=5.+i,cap_pct_of_team=.01))
    def search(frame,groups):
        model=MagicMock();model.best_params_={'ridge__alpha':1.}
        def predict(test):
            # Capture scorer row keys using distinct feature marker below.
            assert not set(frame.gsis_id)&set(test.total_epa.map(lambda x:f'p{int(x)}'))
            return np.full(len(test),np.log1p(1000))
        model.predict.side_effect=predict
        return model
    data=pd.DataFrame(rows)
    data['total_epa']=data.gsis_id.str[1:].astype(float)
    monkeypatch.setattr(valuation,'_search',search)
    # This test concerns grouped historical scoring; omit chronological rows.
    data['season']=2025
    data=data.drop_duplicates('gsis_id',keep='first')
    extra=data.iloc[[0]].copy();extra['season']=2024;extra['is_rookie_deal']=True
    data.loc[data.gsis_id.eq('p0'),'is_rookie_deal']=False
    data=pd.concat([data,extra],ignore_index=True)
    scored,_=score_completed_seasons(data,2026)
    p0=scored.loc[scored.gsis_id.eq('p0')]
    assert p0.outer_fold.nunique()==1
    assert p0.prediction_provenance.eq('player_grouped_out_of_fold').all()
    assert scored.prediction_was_bounded.all()
    assert (scored.expected_apy/scored.season_cap_m*100 <= scored.prediction_bound_cap_share_pct+1e-10).all()
