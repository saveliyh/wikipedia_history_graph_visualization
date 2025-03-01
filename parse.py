import asyncio
from aiohttp import ClientSession
import logging

logger = logging.getLogger(__name__)


async def get_page_data(**kwargs):
    async with ClientSession() as session:
        logger.info(f"Getting page data with params {kwargs}")
        params = {
            "action": "query",
            "format": "json",
        }
        params.update(kwargs)
        i = 0
        # while True:
        #     try:
        #         i += 1
        #         response = re.get(
        #             url="https://en.wikipedia.org/w/api.php",
        #             params=params,
        #         ).json()
        #         break
        #     except Exception:
        #         logger.warning(f"Failed to get page data made {i} attempts")
        i = 0
        while True:
            try:
                i += 1
                async with session.get(
                    url="https://en.wikipedia.org/w/api.php",
                    params=params,
                ) as response:
                    response = await response.json()
                    break
            except Exception:
                logger.warning(f"Failed to get page {kwargs} data made {i} attempts")
                await asyncio.sleep(0.1)

        if "error" in response:
            raise Exception(response["error"])
        elif "warnings" in response:
            logger.warning(response["warnings"])
        elif "query" in response:
            page = list(response["query"]["pages"].values())[0]
            page.pop("ns", None)
            logger.info(
                f"Got page data with pageid {page['pageid']} and title {page['title']}"
            )
            return page


async def get_links_from_page(**kwargs):
    logger.info(f"Getting links from page with params {kwargs}")
    async with ClientSession() as session:
        params = {
            "action": "query",
            "format": "json",
            "generator": "links",
        }
        params.update(kwargs)
        connected_pages = []
        last_continue = {}
        while True:
            req = params.copy()
            req.update(last_continue)

            i = 0
            while True:
                try:
                    i += 1
                    async with session.get(
                        url="https://en.wikipedia.org/w/api.php",
                        params=req,
                    ) as response:
                        response = await response.json()
                        break
                except Exception:
                    logger.warning(
                        f"Failed to get links from page {kwargs} made {i} attempts"
                    )
                    await asyncio.sleep(0.1)

            if "error" in response:
                raise Exception(response["error"])
            elif "warnings" in response:
                print(response["warnings"])
            elif "query" in response:
                for page in response["query"]["pages"].values():
                    if "pageid" in page:
                        connected_pages.append(page["pageid"])

            if "continue" not in response:
                break

            last_continue = response["continue"]

        logger.info(f"Got {len(connected_pages)} links from page with params {kwargs}")
        return connected_pages
