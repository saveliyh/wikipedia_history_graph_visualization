from skytable_py import Config, Query, UInt
import logging
import json

logger = logging.getLogger(__name__)


def prepare_data(values):
    for value in values:
        if isinstance(value, list):
            yield from prepare_data(value)
        elif isinstance(value, int):
            yield UInt(value)
        elif isinstance(value, str):
            yield value.encode("utf-8")
        else:
            yield value


async def insert(db: Config, space: str, model: str, data: dict):

    string = "".join(
        [
            "insert into ",
            space,
            ".",
            model,
            " {\n",
            *[
                (
                    f"{key}: {['?' for _ in value]},\n".replace("'", "")
                    if isinstance(value, list)
                    else f"{key}: ?,\n"
                )
                for key, value in list(data.items())[:-1]
            ],
            (
                f"{list(data.items())[-1][0]}: {['?' for _ in list(data.items())[-1][1]]}\n".replace(
                    "'", ""
                )
                if isinstance(list(data.items())[-1][1], list)
                else f"{list(data.items())[-1][0]}: ?\n"
            ),
            "}",
        ]
    )
    query = Query(
        string,
        *prepare_data(data.values()),
    )

    result = await db.run_simple_query(query)
    if not result.is_empty():
        if result.error() != 108:

            logger.error(f"Failed to insert {model} with title {data['title']}")
            logger.error(string)
            logger.error(json.dumps(data, indent=4))
            raise AssertionError(result.error())
        else:
            if "title" in data.keys():
                logger.info(f"{model} with title {data['title']} already exists")
            else:
                logger.info(f"{model} already exists")
    else:
        if "title" in data.keys():
            logger.info(f"Inserted {model} with title {data['title']}")
        else:
            logger.info(f"Inserted {model}")


async def get_page(db: Config, params: dict) -> dict | None:

    string = "select * from pages.page where " + " and ".join(
        [f"{key} = ?" for key in params.keys()]
    )
    query = Query(string, *prepare_data([value for value in params.values()]))
    row = (await db.run_simple_query(query)).row()
    if row is None:
        return None
    dict_result = dict()
    dict_result["id"], dict_result["title"], dict_result["connected"] = map(
        lambda x: x.data(), row.columns
    )
    dict_result["connected"] = list(map(lambda x: x.data(), dict_result["connected"]))
    dict_result["title"] = dict_result["title"].decode("utf-8")
    logger.debug(
        f"Got page data with pageid {dict_result['id']} and title {dict_result['title']}"
    )
    return dict_result


async def get_metadata(db: Config) -> set[int]:
    string = "select pages from pages.metadata where id = ?"
    query = Query(string, UInt(0))
    row = await db.run_simple_query(query)
    if row.error() == 111:
        return set()
    return set(map(lambda x: x.int(), row.row().columns[0].data()))


async def sync_metadata(db: Config, checked_pages: set[int]):
    pages_in_db = await get_metadata(db)
    logger.info(
        f"syncing metadata with {len(checked_pages)} total pages and {len(pages_in_db)} pages in db"
    )

    for page in checked_pages.difference(pages_in_db):
        string = "update pages.metadata set pages += ? where id = ?"
        query = Query(string, UInt(page), UInt(0))

        result = await db.run_simple_query(query)

        if result.error():

            logger.error("Failed to add update metadata")
            logger.error(string)
            raise AssertionError(result.error())
        else:
            logger.info("Metadata updated")


async def init(db: Config, space: str, model: str, columns: dict):
    res = await db.run_simple_query(
        Query(
            "create space if not exists " + space,
        )
    )

    if res.value().data():
        logger.info(f"Space {space} created")
    else:
        logger.info(f"Space {space} already exists")

    res = await db.run_simple_query(
        Query(
            "create model if not exists "
            + space
            + "."
            + model
            + "("
            + ",".join([f"{key}: {value}" for key, value in columns.items()])
            + ")"
        )
    )
    # print(res.value())
    if res.value().data():
        logger.info(f"Model {model} created")
    else:
        logger.info(f"Model {model} already exists")
