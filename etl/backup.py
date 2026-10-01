"""Read-only paginated backup of retained dashboard rows before history work."""
import argparse
from pathlib import Path
from datetime import datetime,timezone
import json
from dotenv import load_dotenv
import pandas as pd
from etl.database import get_supabase_client
from etl.cloud_store import CloudStore,read_pages


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--local',help='Write backup locally before the first migration')
    args=parser.parse_args(argv)
    load_dotenv()
    client=get_supabase_client()
    rows=read_pages(client.table('roster_roi').select('*').order('season').order('gsis_id').order('id'))
    meta=client.table('pipeline_meta').select('*').execute().data
    timestamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    if args.local:
        folder=Path(args.local);folder.mkdir(parents=True,exist_ok=True)
        pd.DataFrame(rows).to_parquet(folder/f'roster_roi_{timestamp}.parquet',index=False)
        (folder/f'pipeline_meta_{timestamp}.json').write_text(json.dumps(meta))
        print(f'Backed up {len(rows)} rows locally')
    else:
        store=CloudStore(client)
        with store.lock():
            manifest=dict(kind='pre-history-backup',created_at=timestamp,rows=len(rows),
                          metrics=store.put_frame(pd.DataFrame(rows)),metadata=store.put_json(meta))
            descriptor=store.put_json(manifest)
            # A durable catalog reference allows recovery without runner logs.
            client.table('pipeline_backups').insert(dict(manifest=descriptor,row_count=len(rows))).execute()
        print(f'Archived backup of {len(rows)} rows; manifest SHA256 {descriptor["sha256"]}')


if __name__=='__main__':
    main()
