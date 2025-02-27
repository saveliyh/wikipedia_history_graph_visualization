from parse import get_links_from_page


import json

if __name__ == "__main__":

    pages = get_links_from_page("Derek Kolstad")

    with open("data.txt", "w") as w:
        for json_data in pages:
            print(json.dumps(json_data, indent=4, sort_keys=True), file=w)
