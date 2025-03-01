from skytable_py import Config, Query, UInt
import logging

logger = logging.getLogger(__name__)


async def insert(db: Config, space: str, model: str, data: dict):
    def prepare_data(values):
        for value in values:
            if isinstance(value, list):
                yield from prepare_data(value)
            elif isinstance(value, int):
                yield UInt(value)
            else:
                yield value

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
    # print(data)
    # print(*list(map(type, prepare_data(data.values()))), sep="\n")
    result = await db.run_simple_query(query)
    if not result.is_empty():
        if result.error() != 108:
            raise AssertionError(result.error())
    logger.info(f"Inserted {model} with title {data['title']}")


async def select(db: Config, rows: list[str], **kwargs) -> dict:
    pass


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
    if res.value().data():
        logger.info(f"Model {model} created")
    else:
        logger.info(f"Model {model} already exists")
