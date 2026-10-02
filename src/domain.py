"""NFL assumptions shared by analysis and presentation; no external I/O."""

import pandas as pd

OFFENSIVE_POSITIONS = ("QB", "RB", "WR", "TE")
FIRST_SEASON = 2021
POSITION_FAMILIES = {
    'QB':'QB', 'RB':'RB', 'HB':'RB', 'FB':'RB', 'WR':'WR', 'TE':'TE',
    'OL':'OL', 'LT':'OL', 'RT':'OL', 'LG':'OL', 'RG':'OL', 'C':'OL', 'OT':'OL', 'G':'OL',
    'DL':'DL', 'IDL':'DL', 'DT':'DL', 'DE':'DL', 'EDGE':'DL',
    'LB':'LB', 'ILB':'LB', 'OLB':'LB', 'DB':'DB', 'CB':'DB', 'S':'DB', 'FS':'DB', 'SS':'DB',
    'K':'K', 'P':'P', 'LS':'LS',
}
OTC_TEAM_CODES = dict(zip(
    ['Cardinals','Falcons','Ravens','Bills','Panthers','Bears','Bengals','Browns',
     'Cowboys','Broncos','Lions','Packers','Texans','Colts','Jaguars','Chiefs',
     'Raiders','Chargers','Rams','Dolphins','Vikings','Patriots','Saints','Giants',
     'Jets','Eagles','Steelers','49ers','Seahawks','Buccaneers','Titans','Commanders'],
    ['ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE','DAL','DEN','DET','GB','HOU','IND',
     'JAX','KC','LV','LAC','LA','MIA','MIN','NE','NO','NYG','NYJ','PHI','PIT','SF','SEA','TB','TEN','WAS']))
OTC_TEAM_CODES.update({c:c for c in OTC_TEAM_CODES.values()})
OTC_TEAM_CODES.update({'LAR':'LA','Washington':'WAS','Redskins':'WAS','Football Team':'WAS','WSH':'WAS'})
# Millions of dollars per club. Source: https://www.nfl.com/news/nfl-salary-cap
SALARY_CAP_MILLIONS = {
    2021: 182.5, 2022: 208.2, 2023: 224.8,
    2024: 255.4, 2025: 279.2, 2026: 301.2,
}


def numeric(values, fill=0.0):
    """Coerce numeric inputs without coupling calculations to the ETL package."""
    result = pd.to_numeric(values, errors="coerce")
    if hasattr(result, "fillna"):
        return result.fillna(fill)
    return fill if pd.isna(result) else result


def rookie_contract_mask(frame, season):
    """Approximate cohorts; roster experience is not CBA accrued seasons."""
    signed = numeric(frame["year_signed"], fill=float("nan"))
    draft_year = numeric(frame["draft_year"], fill=float("nan"))
    draft_round = numeric(frame["draft_round"], fill=float("nan"))
    entry_year = numeric(frame["entry_year"], fill=float("nan"))
    experience = numeric(frame["years_exp"], fill=float("nan"))
    experience = experience.fillna(season - entry_year)
    drafted = draft_round.between(1, 7)
    window = (season - draft_year).between(0, 3) | (
        draft_round.eq(1) & (season - draft_year).eq(4)
    )
    initial_deal = drafted & signed.eq(draft_year) & window
    udfa = ~drafted & experience.between(0, 2) & signed.eq(entry_year)
    return (initial_deal | udfa).fillna(False)
