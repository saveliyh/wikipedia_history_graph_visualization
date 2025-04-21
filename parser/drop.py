from skytable_py import Config, Query
import asyncio

c = Config("root", "dev_password_will_be_changed", host="localhost", port=2003)


async def drop():
    db = None

    try:
        db = await c.connect()
        await db.run_simple_query(Query("drop model allow not empty pages.metadata"))
    except Exception as e:
        print(e)
    finally:
        if db:
            await db.close()


asyncio.run(drop())
