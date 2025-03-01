from parse import get_links_from_page, get_page_data
from random import choice, random
import logging
import asyncio
from skytable_py import Config, Query, UInt
from db import insert, init

logging.basicConfig(
    level=logging.INFO,
    filename="log.log",
    filemode="a",
    format="%(asctime)s %(levelname)s %(message)s",
)

c = Config("root", "77a23dea-64f9-48", host="localhost", port=2003)


async def main():
    db = None
    try:
        # probability to choose DFS otherwise BFS
        balance = 0.7
        db = await c.connect()
        await init(
            db,
            "pages",
            "page",
            {"id": "uint64", "title": "binary", "connected": "list {type: uint64}"},
        )
        # start with Main Page
        current = 217225
        # current = 8857668
        close_pages = set()
        checked_pages = set()
        while True:

            pages = asyncio.create_task(get_links_from_page(pageids=current))
            page = asyncio.create_task(get_page_data(pageids=current))
            pages = await pages
            page = await page
            page["id"] = page.pop("pageid", None)

            page["connected"] = pages
            waiting_insert = asyncio.create_task(insert(db, "pages", "page", page))
            checked_pages.add(current)

            if len(pages) == 0:
                if len(close_pages) == 0:
                    await waiting_insert
                    logging.info("cannot find more connected pages")
                    break
                else:
                    while current in checked_pages:
                        current = close_pages.pop()
                        logging.info(f"choose {current} for BFS")
                        if len(checked_pages) == 0:
                            await waiting_insert
                            logging.info("cannot find more connected pages")
                            break

            elif len(close_pages) == 0:
                while current in checked_pages:
                    current = choice(pages)
                    logging.info(f"choose {current} for DFS")
                    pages.remove(current)
                    if len(pages) == 0:

                        await waiting_insert
                        logging.info("cannot find more connected pages")
                        break

            elif random() < balance:
                while current in checked_pages:
                    current = choice(pages)
                    logging.info(f"choose {current} for DFS")
                    pages.remove(current)
                    if len(pages) == 0:
                        if len(close_pages) == 0:
                            await waiting_insert
                            logging.info("cannot find more connected pages")
                            break
                        else:
                            while current in checked_pages:
                                current = close_pages.pop()
                                logging.info(f"choose {current} for BFS")
                                if len(close_pages) == 0:
                                    await waiting_insert
                                    logging.info("cannot find more connected pages")
                                    break

            else:
                while current in checked_pages:
                    current = close_pages.pop()
                    logging.info(f"choose {current} for BFS")
                    if len(pages) == 0:
                        await waiting_insert
                        logging.info("cannot find more connected pages")
                        break
                    else:
                        while current in checked_pages:
                            current = choice(pages)
                            logging.info(f"choose {current} for DFS")
                            pages.remove(current)
                            if len(pages) == 0:
                                await waiting_insert
                                logging.info("cannot find more connected pages")
                                break

            close_pages.update(pages)
            await waiting_insert

    except Exception as e:
        logging.exception(f"Failed with error {e}")
    finally:
        if db:
            await db.close()


async def get():
    db = None
    try:
        db = await c.connect()
        row = await db.run_simple_query(
            Query("""select * from pages.page where id = ?""", UInt(1))
        )

    except Exception as e:
        print("Failed with error", e)
    finally:
        if db:
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
