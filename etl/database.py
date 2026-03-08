import os
import datetime
import pandas as pd

from loguru import logger
from supabase import create_client

from etl.config import DEFAULT_BATCH

def get_supabase_client():
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    if not url or not key:
        raise RuntimeError('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment')
    return create_client(url, key)

def upsert_supabase(supabase, df: pd.DataFrame, table: str = 'roster_roi', batch_size: int = DEFAULT_BATCH) -> int:
    raw_records = df.to_dict(orient='records')
    
    records = []
    for row in raw_records:
        clean_row = {}
        for k, v in row.items():
            # pd.isna() catches NaN, pd.NA, NaT, and None
            if pd.isna(v):
                clean_row[k] = None
            else:
                clean_row[k] = v
        records.append(clean_row)

    total = len(records)
    logger.info('Upserting {} records to Supabase table \'{}\'', total, table)
    
    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        resp = supabase.table(table).upsert(batch, on_conflict='ux_roster_roi_season_gsis').execute()

        try:
            code = getattr(resp, 'status_code', None)
            if code and code >= 400:
                logger.error('Supabase upsert error status {}: {}', code, getattr(resp, 'data', resp))
                raise RuntimeError('Supabase upsert failed')
        except Exception:
            pass
            
    logger.info('Upsert complete')
    return total

def update_pipeline_meta(supabase, status: str, row_count: int = 0, message: str = ""):
    payload = {
        'id': 1,
        'last_run': datetime.datetime.now(datetime.UTC).isoformat(),
        'last_row_count': int(row_count),
        'last_status': status,
        'last_message': message or "",
    }
    try:
        supabase.table('pipeline_meta').upsert(payload).execute()
    except Exception as e:
        logger.warning('Failed to update pipeline_meta: {}', e)
