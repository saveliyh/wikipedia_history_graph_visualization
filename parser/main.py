from parse import get_links_from_page, get_page_data
from random import choice, random
import logging
import asyncio
from skytable_py import Config
from db import insert, init, get_page, sync_metadata
from threading import Thread

logging.basicConfig(
    level=logging.INFO,
    filename="log.log",
    filemode="a",
    format="%(asctime)s %(levelname)s %(message)s",
)
# TODO: Make secure
c = Config("root", "dev_password_will_be_changed", host="localhost", port=2003)


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
        await init(
            db,
            "pages",
            "metadata",
            {"id": "uint8", "pages": "list {type: uint64}"},
        )
        await insert(db, "pages", "metadata", {"id": 0, "pages": [217225]})
        while True:
            if len(pages_list) == 0:
                await asyncio.sleep(10)
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


async def run_tasks(tasks: list, pages_list: list[tuple[int, dict]], close_pages: set):
    pages = []
    for current, task in tasks:
        pages = await task
        pages_list.append((current, pages))
        close_pages.update(pages)

    return pages


async def get_data(pages_list: list[tuple[int, dict]]):
    return pages_list


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

        await init(
            db,
            "pages",
            "metadata",
            {"id": "uint8", "pages": "list {type: uint64}"},
        )
        await insert(db, "pages", "metadata", {"id": 0, "pages": [217225]})
        balance = 0.1
        # start with Main Page
        current = 217225
        # current = 8857668
        close_pages = set()
        checked_pages = set()
        calls = []
        while True:
            if len(calls) >= 100:
                await run_tasks(calls, pages_list, close_pages)
            data_from_db = await get_page(db, {"id": current})
            if data_from_db is not None:
                logging.info(
                    f"page {current} with title {data_from_db['title']} already exists in DB"
                )

                pages = asyncio.create_task(get_data(data_from_db["connected"]))

            else:
                pages = asyncio.create_task(get_links_from_page(pageids=current))

            calls.append((current, pages))

            checked_pages.add(current)
            await sync_metadata(db, checked_pages)

            if len(close_pages) == 0:
                pages = await run_tasks(calls, pages_list, close_pages)
                while current in checked_pages:
                    if len(pages) == 0:

                        logging.info("cannot find more connected pages")
                        return
                    current = choice(pages)
                    logging.debug(f"choose {current} for DFS")
                    pages.remove(current)

            elif random() < balance:
                pages = await run_tasks(calls, pages_list, close_pages)
                while current in checked_pages:
                    current = choice(pages)
                    logging.debug(f"choose {current} for DFS")
                    pages.remove(current)
                    if len(pages) == 0:
                        if len(close_pages) == 0:

                            logging.info("cannot find more connected pages")
                            return
                        else:
                            while current in checked_pages:
                                current = close_pages.pop()
                                logging.debug(f"choose {current} for BFS")
                                if len(close_pages) == 0:

                                    logging.info("cannot find more connected pages")
                                    break

            else:
                while current in checked_pages:
                    current = close_pages.pop()
                    logging.debug(f"choose {current} for BFS")
                    if len(close_pages) == 0:
                        pages = await run_tasks(calls, pages_list, close_pages)
                        if len(pages) == 0:

                            logging.info("cannot find more connected pages")
                            return
                        else:
                            while current in checked_pages:
                                pages = await run_tasks(calls, pages_list, close_pages)
                                current = choice(pages)
                                logging.debug(f"choose {current} for DFS")
                                pages.remove(current)
                                if len(pages) == 0:

                                    logging.info("cannot find more connected pages")
                                    break

    except Exception as e:

        logging.exception(f"Failed in parsing thread with error {e}")
    finally:
        pages_list.append((-1, None))
        if db:
            await db.close()


async def main():

    try:
        pages_list = []
        # input_thread = Thread(
        #     target=asyncio.run,
        #     args=[insert_page(pages_list)],
        # )
        # parse_thread = Thread(
        #     target=asyncio.run,
        #     args=[parse_pages(pages_list)],
        # )
        insert_task = asyncio.create_task(insert_page(pages_list))
        parse_task = asyncio.create_task(parse_pages(pages_list))
        await insert_task
        await parse_task

        # input_thread.start()
        # parse_thread.start()

        # input_thread.join()
        # parse_thread.join()
    except Exception as e:
        logging.exception(f"Failed in main with error {e}")
    finally:
        pages_list.append((-1, None))


if __name__ == "__main__":
    asyncio.run(main())
