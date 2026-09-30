"""Small database availability check for an external scheduler."""

from dotenv import load_dotenv
from loguru import logger
from etl.database import check_connection, get_supabase_client


def main():
    load_dotenv()
    check_connection(get_supabase_client())
    logger.info('Supabase database query succeeded')


if __name__ == '__main__':
    main()
