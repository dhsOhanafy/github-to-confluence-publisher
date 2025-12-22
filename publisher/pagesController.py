import json
import logging
import requests
from urllib.parse import quote
from urllib3.exceptions import InsecureRequestWarning
from requests.auth import HTTPBasicAuth
from config.getconfig import getConfig

CONFIG = getConfig()



# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

#
# Function to check if page exists by title and parent
#
def findPageByTitle(title, parentPageID, login, password):
    """
    Search for existing page by exact title and parent.
    Returns page dict with 'id' and 'version' if found, None otherwise.
    """
    # Support both typo and correct spelling for backwards compatibility
    parent_id = CONFIG.get("confluence_parent_page_id") or CONFIG.get("counfluence_parent_page_id")
    if parentPageID is None:
        parent_id_to_use = parent_id
    else:
        parent_id_to_use = str(parentPageID)

    # Build search query - exact title match with parent constraint
    # CQL: title="exact title" AND parent={id} AND space="key"
    search_title = title + "  " + str(CONFIG["confluence_search_pattern"])
    cql_query = f'title="{search_title}" AND parent={parent_id_to_use} AND space="{CONFIG["confluence_space"]}"'

    # URL-encode the CQL query to handle special characters
    encoded_cql = quote(cql_query)

    logging.debug(f"Searching for existing page: {cql_query}")
    logging.debug(f"Encoded CQL: {encoded_cql}")

    try:
        response = requests.get(
            url=f'{CONFIG["confluence_url"]}search?cql={encoded_cql}&limit=5&expand=version',
            auth=HTTPBasicAuth(login, password),
            verify=False
        )

        if response.status_code == 200:
            results = json.loads(response.text)
            if results.get('size', 0) > 0:
                # Found existing page
                page_data = results['results'][0]['content']
                logging.info(f"Found existing page: {page_data['id']} (v{page_data['version']['number']})")
                return {
                    'id': page_data['id'],
                    'version': page_data['version']['number'],
                    'title': page_data['title']
                }

        logging.debug("No existing page found")
        return None

    except Exception as e:
        logging.warning(f"Error searching for existing page: {e}")
        return None


#
# Function for UPDATE-or-CREATE page with CONTENT (idempotent)
#
def createPage(title, content, parentPageID, login, password):
    """
    Idempotent page publishing: Updates existing page or creates new one.
    This replaces the old CREATE-only logic to eliminate duplicate page errors.
    """

    # Build page title with search pattern
    full_title = title + "  " + str(CONFIG["confluence_search_pattern"])

    # Add autogeneration warning banner
    banner = "<p style=\"background-color:#e7be17;\">⚠️ This page is auto-generated from OHFY-Core-AI repository. " + \
             "Do not edit manually - changes will be overwritten. " + \
             "<b><a href=\"https://github.com/ohanafy/OHFY-Core-AI\">View source on GitHub</a></b></p>"
    full_content = banner + content

    # Check if page already exists
    existing_page = findPageByTitle(title, parentPageID, login, password)

    if existing_page:
        # UPDATE existing page
        logging.info(f"Updating existing page: {full_title} (ID: {existing_page['id']})")
        return updatePage(
            page_id=existing_page['id'],
            title=full_title,
            content=full_content,
            version=existing_page['version'],
            login=login,
            password=password
        )
    else:
        # CREATE new page
        logging.info(f"Creating new page: {full_title}")
        return createNewPage(
            title=full_title,
            content=full_content,
            parentPageID=parentPageID,
            login=login,
            password=password
        )


#
# Function to UPDATE existing page
#
def updatePage(page_id, title, content, version, login, password):
    """
    Update existing Confluence page via PUT request.
    """
    update_payload = {
        "id": page_id,
        "type": "page",
        "title": title,
        "version": {
            "number": version + 1  # Increment version
        },
        "body": {
            "storage": {
                "value": content,
                "representation": "storage"
            }
        }
    }

    logging.debug(f"Updating page {page_id} to version {version + 1}")

    try:
        response = requests.put(
            url=f'{CONFIG["confluence_url"]}content/{page_id}',
            json=update_payload,
            auth=HTTPBasicAuth(login, password),
            verify=False
        )

        logging.debug(f"Update response status: {response.status_code}")

        if response.status_code == 200:
            response_json = json.loads(response.text)
            logging.info(f"Updated successfully (v{version + 1})")
            return {
                'success': True,
                'page_id': page_id,
                'operation': 'updated'
            }
        else:
            response_json = json.loads(response.text)
            error_message = response_json.get('message', 'Unknown error')
            logging.error(f"Update failed: {error_message}")
            return {
                'success': False,
                'error': error_message,
                'status_code': response.status_code,
                'response': response_json
            }

    except Exception as e:
        logging.error(f"Exception during page update: {e}")
        return {
            'success': False,
            'error': str(e),
            'status_code': 'exception'
        }


#
# Function to CREATE new page
#
def createNewPage(title, content, parentPageID, login, password):
    """
    Create new Confluence page via POST request.
    """
    # descripe json query
    newPageJSONQueryString = """
    {
        "type": "page",
        "title": "DEFAULT PAGE TITLE",
        "ancestors": [
            {
            "id": 111
            }
        ],
        "space": {
            "key": "DEFAULT KEY"
        },
        "body": {
            "storage": {
                "value": "DEFAULT PAGE CONTENT",
                "representation": "storage"
            }
        }
    }
    """

    # load json from string
    newPagejsonQuery = json.loads(newPageJSONQueryString)

    # the key of Confluence space for content publishing
    newPagejsonQuery['space']['key'] = CONFIG["confluence_space"]

    # check of input of the ParentPageID
    if parentPageID is None:
        # Support both typo and correct spelling for backwards compatibility
        parent_id = CONFIG.get("confluence_parent_page_id") or CONFIG.get("counfluence_parent_page_id")
        newPagejsonQuery['ancestors'][0]['id']  = parent_id # this is the root of out pages tree
    else:
        newPagejsonQuery['ancestors'][0]['id'] = str(parentPageID) # this is the branch of our tree

    newPagejsonQuery['title'] = title
    newPagejsonQuery['body']['storage']['value'] = content

    logging.debug(f"Creating page: {title}")
    logging.debug(json.dumps(newPagejsonQuery, indent=4, sort_keys=True))

    # make call to create new page
    response = requests.post(
        url=CONFIG["confluence_url"] + "content/",
        json=newPagejsonQuery,
        auth=HTTPBasicAuth(login, password),
        verify=False)

    logging.debug(response.status_code)

    # Parse response
    try:
        response_json = json.loads(response.text)
        logging.debug(json.dumps(response_json, indent=4, sort_keys=True))
    except json.JSONDecodeError:
        logging.error("Failed to parse response JSON")
        return {
            'success': False,
            'error': f'Invalid JSON response from Confluence (status {response.status_code})',
            'response_text': response.text[:500]  # First 500 chars for debugging
        }

    # Check if page was created successfully
    if response.status_code == 200 and 'id' in response_json:
        logging.info("Created successfully")
        page_id = response_json['id']
        logging.debug("Returning created page id: " + page_id)
        return {
            'success': True,
            'page_id': page_id,
            'operation': 'created'
        }
    else:
        # Page creation failed
        error_message = response_json.get('message', 'Unknown error')
        logging.error(f"Page creation failed: {error_message}")
        return {
            'success': False,
            'error': error_message,
            'status_code': response.status_code,
            'response': response_json
        }


#
# Function for searching pages with SEARCH TEST in the title
#
def searchPages(login, password):
    """
    Search for all autogenerated pages with pagination support.

    Confluence API caps results at 250 per request, so we use pagination
    to fetch all pages across multiple requests.
    """
    # Support both typo and correct spelling for backwards compatibility
    parent_id = CONFIG.get("confluence_parent_page_id") or CONFIG.get("counfluence_parent_page_id")

    # Initial CQL query URL with limit=250 (Confluence's max per-page limit)
    base_cql = f'title~{{"{CONFIG["confluence_search_pattern"]}"}}+and+type=page+and+space="{CONFIG["confluence_space"]}"'
    initial_url = f'{CONFIG["confluence_url"]}search?cql={base_cql}&limit=250'

    foundPages = []
    current_url = initial_url
    page_count = 0

    logging.info(f"Searching for pages with pattern: {CONFIG['confluence_search_pattern']}")

    # Paginate through all results
    while current_url:
        page_count += 1
        logging.debug(f"Fetching page {page_count}: {current_url}")

        try:
            response = requests.get(
                url=current_url,
                auth=HTTPBasicAuth(login, password),
                verify=False,
                timeout=30
            )

            if response.status_code != 200:
                logging.error(f"Search failed with status {response.status_code}")
                logging.error(f"Response: {response.text[:500]}")
                break

            results = response.json()

            # Extract page IDs from this batch
            batch_size = len(results.get('results', []))
            for result in results.get('results', []):
                page_id = result['content']['id']
                page_title = result['content']['title']
                foundPages.append(page_id)
                logging.debug(f"Found page: {page_id} - {page_title}")

            total_size = results.get('totalSize', 0)
            current_count = len(foundPages)
            logging.info(f"Batch {page_count}: Retrieved {batch_size} pages ({current_count}/{total_size} total)")

            # Check for next page
            next_link = results.get('_links', {}).get('next')
            if next_link:
                # Next link is relative path like "/rest/api/search?..."
                # Construct full URL from base domain
                base_link = results.get('_links', {}).get('base', '')
                if base_link:
                    current_url = base_link + next_link
                else:
                    # Fallback: extract domain from confluence_url
                    domain = CONFIG["confluence_url"].split('/rest/')[0]
                    current_url = domain + next_link
                logging.debug(f"More results available, following pagination to: {current_url}")
            else:
                logging.info(f"All pages retrieved: {current_count} total")
                current_url = None  # Exit loop

        except Exception as e:
            logging.error(f"Error during pagination: {e}")
            break

    logging.info(f"Found {len(foundPages)} pages in space {CONFIG['confluence_space']} " +
                f"with search pattern: {CONFIG['confluence_search_pattern']}")

    return foundPages


#
# Function for deleting pages
# 
def deletePages(pagesIDList, login, password):


    deletedPages = []


    for page in pagesIDList:
        logging.info("Delete page: " + str(page))
        logging.debug("Calling URL: " + str(CONFIG["confluence_url"]) + "content/" + str(page))
        response = requests.delete(
            url=str(CONFIG["confluence_url"]) + "content/" + str(page),
            auth=HTTPBasicAuth(login, password),
            verify=False)
        logging.debug("Delete status code: " + str(response.status_code))
        if response.status_code == 204:
            logging.info("Deleted successfully")

    return deletedPages

#   
# Function for attaching file
# 
def attachFile(pageIdForFileAttaching, attachedFile, login, password):
 
    # make call to attache fale to a page
    logging.debug("Calling URL: " + str(CONFIG["confluence_url"]) + "content/" + str(pageIdForFileAttaching) + "/child/attachment")

    attachedFileStructure = {'file': attachedFile}
    attachedValues = {'comment': 'file was attached by the script'}
    attachedHeader=  {"Accept": "application/json",
                        "X-Atlassian-Token": "nocheck"} # disable token check. Otherwise it will be 443 status code

    response = requests.post(
        url=CONFIG["confluence_url"] + "content/" + str(pageIdForFileAttaching) + "/child/attachment",
        files=attachedFileStructure,
        data=attachedValues,
        auth=HTTPBasicAuth(login, password),
        headers=attachedHeader,
        verify=False)

    logging.debug(response.status_code)
    if response.status_code == 200:
        logging.info("File was attached successfully")
        logging.debug(json.dumps(json.loads(response.text), indent=4, sort_keys=True))

        # return id of the attached file
        logging.debug("Returning attached file id: " + json.loads(response.text)['results'][0]['id'])
        return json.loads(response.text)['results'][0]['id']
    else:
        logging.error("File has not attached")
