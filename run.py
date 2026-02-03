import os
import asyncio
from nthrow.utils import create_db_connection, create_store
from sample_extractor import MyExtractor


creds = {
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ["DB"],
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"],
}

conn = create_db_connection(**creds)
create_store(conn, "nthrows")

extractor = MyExtractor(conn, "nthrows")
extractor.set_list_info("https://www.scrapethissite.com/pages/forms/")

async def main():
    async with await extractor.create_session() as session:
        extractor.session = session
        await extractor.collect_rows(extractor.get_list_row())
        row = extractor.get_list_row()
        print("Collected! Pagination state:", row["state"])

asyncio.run(main())