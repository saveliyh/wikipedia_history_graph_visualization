from parse import get_links_from_page, get_page_data
import json
import asyncio
from skytable_py import Config, Query

c = Config("root", "661caa49-74a5-44", host="localhost", port=2003)


async def main():
    db = None
    try:
        db = await c.connect()
        pages = await get_links_from_page(titles="Derek Kolstad")
        page = await get_page_data(titles="Derek Kolstad")
        # init space
        assert (
            await db.run_simple_query(Query("create space if not exists pages"))
        ).is_empty()

        # init model
        assert (
            await db.run_simple_query(
                Query(
                    """create model if not exists pages.page(
                      id: uint16,
                      title: string,
                      connected: list {type: uint16}
                      )"""
                )
            )
        ).is_empty()

        # insert data

        assert await db.run_simple_query(
            Query("""insert into pages.page(id, title, connected) values (?, ?, ?)"""),
            page["pageid"],
            page["title"],
            [link["pageid"] for link in pages],
        )

    except Exception as e:
        print(e)
    finally:
        if db:
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
