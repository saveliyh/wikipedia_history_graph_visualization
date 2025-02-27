import requests as re


def get_links_from_page(page_title="Main Page"):
    params = {
        "action": "query",
        "format": "json",
        "titles": page_title,
        "generator": "links",
    }
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
        print(response)
    return connected_pages
