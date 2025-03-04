from skytable_py import Config, Query, UInt
import asyncio

c = Config("root", "77a23dea-64f9-48", host="localhost", port=2003)


async def drop():
    try:
        db = await c.connect()
        await db.run_simple_query(Query("drop space allow not empty pages"))
    except Exception as e:
        print(e)
    finally:
        await db.close()


asyncio.run(drop())
