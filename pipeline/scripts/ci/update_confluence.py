import copy
import json
import pdb

import xmltodict
from confluence.base import Confluence
from docopt import docopt

doc = """
    This script updates the confluence page titled "RHCS Test Execution Summary"
    with the results of execution given as input.

    Usage:
        update_confluence.py --content <resultJson> --token <token> --title <pageTitle> --space <confSpace>

        update_confluence.py (-h | --help)

    Options:
        -h --help          Shows the command usage
        -c --content resultJson     The results to be appended to the confluence page's table in json format.
                                            Ex: '{"RHCS Version": "RHCS 5.2", "Ceph Version": "16.2.8-79",
                                                "tier-0-sanity_run": "PASS", "tier-1-sanity_run": "FAIL",
                                                "tier-2-sanity_run": "SKIP", "tier-1-scheduled_run": "SKIP"} '
        --token token          The auth token to be used to connect to confluence
        --title pageTitle      The title of the page to be updated
        --space confSpace      The space in confluence under which the page is present.
"""


def get_updated_page_body(pageBody, content):
    """
    This method fetches the updated content after updating the existing content of the page
    "RHCS Test Execution Summary" with results of new version's execution passed in content parameter
    Args:
        pageBody: existing content of the page
        content: new content to be appended to the existing

    Returns:
        None
    """
    pdb.set_trace()
    if not pageBody:
        # If page is empty append add a help text
        data = (
            "<p><strong>NOTE: </strong><strong>DO NOT edit this Page from UI. Results are updated automatically "
            "after each Jenkins CI run.</strong></p><p><br /></p> "
        )
    elif "<table" not in pageBody:
        # If page does not contain a table, create one and fill it with the contents passed
        tableHeader = {"th": []}
        tableRow = {"td": []}
        for key, value in content.items():
            tableHeader["th"].append(key)
            tableRow["td"].append(value)
        rowList = [tableHeader, tableRow]
        tableDict = {"table": {"tbody": {"tr": rowList}}}
        newTable = xmltodict.unparse(tableDict).split("\n")[1]
        data = pageBody + newTable
    else:
        # If page and table exist, then append new content to the table on top.
        existingContent = pageBody.split("<table")[0]
        existingTable = (
            "<table" + pageBody.split("<table")[1].split("</table>")[0] + "</table>"
        )
        tableDict = xmltodict.parse(existingTable)
        tableHeaders = tableDict["table"]["tbody"]["tr"][0]
        tableRows = tableDict["table"]["tbody"]["tr"][1:]
        tableRow = copy.deepcopy(tableRows)[0]
        new_ceph_version = content["Ceph Version"]
        ceph_version_exists = [
            [rIdx, rVal]
            for rIdx, rVal in enumerate(tableRows)
            if new_ceph_version in rVal["td"]
        ]
        for key, value in content.items():
            if key not in tableHeaders["th"]:
                tableHeaders["th"].append(key)
                for rowIdx, rowVal in enumerate(tableRows):
                    if ceph_version_exists and rowIdx == ceph_version_exists[0][0]:
                        tableRows[rowIdx]["td"].append(value)
                    else:
                        tableRows[rowIdx]["td"].append("")
                tableRow["td"].append(value)
            else:
                index = tableHeaders["th"].index(key)
                if ceph_version_exists:
                    rIdx = ceph_version_exists[0][0]
                    tableRows[rIdx]["td"][index] = value
                tableRow["td"][index] = value
        newTableRows = (
            [tableHeaders] + tableRows
            if ceph_version_exists
            else [tableHeaders] + [tableRow] + tableRows
        )
        newTableDict = {"table": {"tbody": {"tr": newTableRows}}}
        newTable = xmltodict.unparse(newTableDict).split("\n")[1]
        data = existingContent + newTable
    return data


def update_confluence_page(content, auth_token, page_title, conf_space):
    """
    This method updates the page "RHCS Test Execution Summary" with the given content
    Args:
        conf_space: The confluence space under which the page is present
        page_title: The title of the page to be updated
        auth_token: The token used to connect to confluence
        content: The results to be appended in the form of json
    Examples::
        content = '{"RHCS Version": "RHCS 5.2", "Ceph Version": "16.2.8-79", "tier-0-sanity_run": "PASS", ' \
               '"tier-1-sanity_run": "FAIL", "tier-2-sanity_run": "SKIP", "tier-3-sanity_run": "SKIP"} '

    Returns:
        None
    """
    client = Confluence(token=auth_token)
    expand = "body.storage,version"
    contentDict = json.loads(content)

    page = client.get_page(page_title, conf_space, expand)
    if page.json() and page.json()["results"]:
        pageContent = page.json()["results"][0]
        pageId = pageContent["id"]
        pageType = pageContent["type"]
        pageVersion = pageContent["version"]["number"] + 1
        pageBody = pageContent["body"]["storage"]["value"]
        pageRepresentation = pageContent["body"]["storage"]["representation"]
        pageBody = get_updated_page_body(pageBody, contentDict)
        updatePageResp = client.update_page(
            title=page_title,
            pageId=pageId,
            spaceKey=conf_space,
            data=pageBody,
            version=pageVersion,
            page_type=pageType,
            representation=pageRepresentation,
        )
        if updatePageResp.status_code != 200:
            print("Error occurred while updating page")
    else:
        print("Error occurred while fetching parent page")


if __name__ == "__main__":
    cli_args = docopt(doc)
    pageContents = cli_args.get("--content")
    title = cli_args.get("--title")
    space = cli_args.get("--space")
    token = cli_args.get("--token")
    try:
        update_confluence_page(
            content=pageContents, auth_token=token, page_title=title, conf_space=space
        )
    except Exception as e:
        raise e
