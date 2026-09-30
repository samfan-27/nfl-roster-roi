import pandas as pd
from loguru import logger
from pathlib import Path

def write_artifacts(df: pd.DataFrame, merged: pd.DataFrame, unmatched: pd.DataFrame, outpath: str):
    Path(outpath).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outpath, index=False)
    logger.info('Wrote artifact CSV to {}', outpath)

    merged_path = Path(outpath).with_name(Path(outpath).stem + '_merged.csv')
    unmatched_path = Path(outpath).with_name(Path(outpath).stem + '_unmatched.csv')
    try:
        merged.to_csv(merged_path, index=False)
        unmatched.to_csv(unmatched_path, index=False)
        logger.info('Wrote merged debug to {} and unmatched mapping to {}', merged_path, unmatched_path)
    except Exception as e:
        logger.warning('Failed to write debug artifacts: {}', e)
