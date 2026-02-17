import os
import asyncio
from nthrow.utils import create_db_connection, create_store
from extractor import HackerNewsExtractor


async def main():
    # credentials
    conn = create_db_connection(
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        database=os.environ.get("DB"),
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT")
    )

    # Create table
    table_name = "hackernews"
    create_store(conn, table_name)
    print(f"Table '{table_name}' created")

    extractor = HackerNewsExtractor(conn, table_name)
    extractor.set_list_info("https://news.ycombinator.com/")

    async with await extractor.create_session() as session:
        extractor.session = session

        # Scrape 3 pages
        for page_num in range(3):
            print(f"\nFetching page {page_num + 1}...")

            row = extractor.get_list_row()
            await extractor.collect_rows(row)

            # stories we have
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE list = false")
                count = cur.fetchone()[0]
                print(f"Total stories: {count}")

            if not extractor.should_run_again():
                break

            extractor._reset_run_times()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())