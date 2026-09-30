import os
import datetime
import pandas as pd
import json

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
    if batch_size < 1:
        raise ValueError('Batch size must be positive')
    records = json.loads(df.to_json(orient='records', date_format='iso'))

    total = len(records)
    logger.info('Upserting {} records to Supabase table \'{}\'', total, table)
    
    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        resp = supabase.table(table).upsert(batch, on_conflict='season,gsis_id').execute()

        code = getattr(resp, 'status_code', None)
        if code and code >= 400:
            raise RuntimeError(f'Supabase upsert failed with status {code}')
            
    logger.info('Upsert complete')
    return total


def check_connection(supabase):
    """Perform a real database read, independently of NFL source availability."""
    response = supabase.table('pipeline_meta').select('id').eq('id', 1).limit(1).execute()
    if not response.data:
        raise RuntimeError('pipeline_meta row 1 is missing or inaccessible')
    return True

def update_pipeline_meta(supabase, status: str, row_count: int = 0, message: str = ""):
    payload = {
        'id': 1,
        'last_run': datetime.datetime.now(datetime.UTC).isoformat(),
        'last_row_count': int(row_count),
        'last_status': status,
        'last_message': message or "",
    }
    supabase.table('pipeline_meta').upsert(payload).execute()
