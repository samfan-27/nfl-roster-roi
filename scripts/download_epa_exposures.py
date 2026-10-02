"""Download public nflverse PBP columns needed to reconcile EPA exposures."""
import argparse
from hashlib import sha256
import json
from pathlib import Path
import requests
import pandas as pd

COLUMNS = ['season', 'season_type', 'week', 'game_id', 'play_type', 'passer_player_id',
           'rusher_player_id', 'receiver_player_id', 'qb_epa', 'epa']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seasons', nargs='+', type=int, default=list(range(2021,2027)))
    parser.add_argument('--output', default='artifacts/methodology-v2/inputs')
    args = parser.parse_args()
    output = Path(args.output); output.mkdir(parents=True, exist_ok=True)
    for year in args.seasons:
        target = output/f'{year}_pbp.parquet'
        if target.exists():
            continue
        url = f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.parquet'
        response = requests.get(url, timeout=180)
        response.raise_for_status()
        full = output/f'{year}_pbp_source.parquet'
        full.write_bytes(response.content)
        data = pd.read_parquet(full, columns=COLUMNS)
        data.to_parquet(target, index=False)
        (output/f'{year}_pbp_provenance.json').write_text(json.dumps(dict(source_url=url, source_sha256=sha256(response.content).hexdigest(),
            source_bytes=len(response.content), fetched_at=pd.Timestamp.now(tz='UTC').isoformat(),
            columns=COLUMNS, subset_sha256=sha256(target.read_bytes()).hexdigest()),indent=2))
        print(f'{year}: PBP source archived and subset saved',flush=True)


if __name__=='__main__':
    main()
