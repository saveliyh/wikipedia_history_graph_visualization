from parse import get_links_from_page, get_page_data
from random import choice, random
import logging
import asyncio
from skytable_py import Config
from db import insert, init, get_page
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    filename="log.log",
    filemode="w",
    format="%(asctime)s %(levelname)s %(message)s",
)

c = Config("root", "77a23dea-64f9-48", host="localhost", port=2003)


async def insert_page(pages_list: list[tuple[int, dict]]):
    db = None
    try:
        db = await c.connect()
        await init(
            db,
            "pages",
            "page",
            {"id": "uint64", "title": "binary", "connected": "list {type: uint64}"},
        )
        while True:
            if len(pages_list) == 0:
                await asyncio.sleep(30)
                continue
            current, pages = pages_list.pop()
            if current == -1:
                break
            page = asyncio.create_task(get_page_data(pageids=current))
            page = await page
            page["id"] = page.pop("pageid", None)

            page["connected"] = pages
            await insert(db, "pages", "page", page)
    except Exception as e:
        logging.exception(f"Failed in input thread with error {e}")
    finally:
        if db:
            await db.close()


async def parse_pages(pages_list: list[tuple[int, dict]]):
    db = None
    try:
        db = await c.connect()
        await init(
            db,
            "pages",
            "page",
            {"id": "uint64", "title": "binary", "connected": "list {type: uint64}"},
        )
        balance = 0.7
        # start with Main Page
        current = 217225
        # current = 8857668
        close_pages = set()
        checked_pages = set()
        while True:
            data_from_db = await get_page(db, {"id": current})
            if data_from_db is not None:
                logging.info(
                    f"page {current} with title {data_from_db['title']} already exists in DB"
                )

                pages = data_from_db["connected"]
            else:
                pages = asyncio.create_task(get_links_from_page(pageids=current))

                pages = await pages
                pages_list.append((current, pages))

            checked_pages.add(current)

            if len(pages) == 0:
                if len(close_pages) == 0:

                    logging.info("cannot find more connected pages")
                    break
                else:
                    while current in checked_pages:
                        current = close_pages.pop()
                        logging.info(f"choose {current} for BFS")
                        if len(checked_pages) == 0:

                            logging.info("cannot find more connected pages")
                            break

            elif len(close_pages) == 0:
                while current in checked_pages:
                    current = choice(pages)
                    logging.info(f"choose {current} for DFS")
                    pages.remove(current)
                    if len(pages) == 0:

                        logging.info("cannot find more connected pages")
                        break

            elif random() < balance:
                while current in checked_pages:
                    current = choice(pages)
                    logging.info(f"choose {current} for DFS")
                    pages.remove(current)
                    if len(pages) == 0:
                        if len(close_pages) == 0:

                            logging.info("cannot find more connected pages")
                            break
                        else:
                            while current in checked_pages:
                                current = close_pages.pop()
                                logging.info(f"choose {current} for BFS")
                                if len(close_pages) == 0:

                                    logging.info("cannot find more connected pages")
                                    break

            else:
                while current in checked_pages:
                    current = close_pages.pop()
                    logging.info(f"choose {current} for BFS")
                    if len(pages) == 0:

                        logging.info("cannot find more connected pages")
                        break
                    else:
                        while current in checked_pages:
                            current = choice(pages)
                            logging.info(f"choose {current} for DFS")
                            pages.remove(current)
                            if len(pages) == 0:

                                logging.info("cannot find more connected pages")
                                break

            close_pages.update(pages)

    except Exception as e:

        logging.exception(f"Failed in parsing thread with error {e}")
    finally:
        pages_list.append((-1, None))
        if db:
            await db.close()


async def main():

    try:
        pages_list = []
        input_thread = Thread(
            target=asyncio.run,
            args=[insert_page(pages_list)],
        )
        parse_thread = Thread(
            target=asyncio.run,
            args=[parse_pages(pages_list)],
        )

        input_thread.start()
        parse_thread.start()

        input_thread.join()
        parse_thread.join()
    except Exception as e:
        logging.exception(f"Failed in main with error {e}")


if __name__ == "__main__":
    asyncio.run(main())
