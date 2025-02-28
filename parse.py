import requests as re


async def get_page_data(**kwargs):
    params = {
        "action": "query",
        "format": "json",
    }
    params.update(kwargs)
    response = re.get(
        url="https://en.wikipedia.org/w/api.php",
        params=params,
    ).json()
    if "error" in response:
        raise Exception(response["error"])
    elif "warnings" in response:
        print(response["warnings"])
    elif "query" in response:
        return response["query"]["pages"].values()[0]


async def get_links_from_page(**kwargs):
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
        response = re.get(
            url="https://en.wikipedia.org/w/api.php",
            params=req,
        ).json()

        if "error" in response:
            raise Exception(response["error"])
        elif "warnings" in response:
            print(response["warnings"])
        elif "query" in response:
            for page in response["query"]["pages"].values():
                connected_pages.append(page)

        if "continue" not in response:
            break

        last_continue = response["continue"]
        # TODO: add logging
    return connected_pages
