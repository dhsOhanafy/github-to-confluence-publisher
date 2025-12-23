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
def findPageByTitleDirect(full_title, login, password):
    """
    Find page by exact title using Content API (more reliable than CQL search).

    This bypasses the search index entirely by using the content API with title filter.
    Much more reliable for eventual consistency issues.

    Returns page dict with 'id' and 'version' if found, None otherwise.
    """
    try:
        # Use content API with title parameter - bypasses search index
        # This is the most reliable way to find a page by exact title
        encoded_title = quote(full_title)
        url = f'{CONFIG["confluence_url"]}content?title={encoded_title}&spaceKey={CONFIG["confluence_space"]}&expand=version'

        logging.debug(f"Direct lookup: {full_title}")

        response = requests.get(
            url=url,
            auth=HTTPBasicAuth(login, password),
            verify=False,
            timeout=15
        )

        if response.status_code == 200:
            results = response.json()
            if results.get('size', 0) > 0:
                page = results['results'][0]
                version_num = page.get('version', {}).get('number', 1)
                logging.info(f"Direct lookup found page: {page['id']} (v{version_num})")
                return {
                    'id': page['id'],
                    'version': version_num,
                    'title': page.get('title', '')
                }

        return None

    except Exception as e:
        logging.warning(f"Direct lookup error: {e}")
        return None


def findPageByTitle(title, parentPageID, login, password):
    """
    Search for existing page by exact title with retry logic.

    Uses ancestor= instead of parent= to find pages at any depth in the hierarchy.
    Implements retry logic to handle Confluence search index eventual consistency.

    Returns page dict with 'id' and 'version' if found, None otherwise.
    """
    import time

    # Support both typo and correct spelling for backwards compatibility
    parent_id = CONFIG.get("confluence_parent_page_id") or CONFIG.get("counfluence_parent_page_id")
    if parentPageID is None:
        parent_id_to_use = parent_id
    else:
        parent_id_to_use = str(parentPageID)

    # Build search query - exact title match with ancestor constraint
    # CQL: title="exact title" AND ancestor={id} AND space="key"
    # NOTE: ancestor= finds pages at ANY depth, not just direct children
    search_title = title + "  " + str(CONFIG["confluence_search_pattern"])
    cql_query = f'title="{search_title}" AND ancestor={parent_id_to_use} AND space="{CONFIG["confluence_space"]}"'

    # URL-encode the CQL query to handle special characters
    encoded_cql = quote(cql_query)

    logging.debug(f"Searching for existing page: {cql_query}")

    # Retry logic to handle eventual consistency
    max_retries = 3
    retry_delays = [0, 2, 4]  # 0s, 2s, 4s delays

    for attempt in range(max_retries):
        try:
            if attempt > 0:
                delay = retry_delays[attempt]
                logging.debug(f"Retry {attempt}/{max_retries-1} after {delay}s delay (eventual consistency)")
                time.sleep(delay)

            response = requests.get(
                url=f'{CONFIG["confluence_url"]}search?cql={encoded_cql}&limit=5&expand=version',
                auth=HTTPBasicAuth(login, password),
                verify=False,
                timeout=10
            )

            if response.status_code == 200:
                results = json.loads(response.text)
                if results.get('size', 0) > 0:
                    # Found existing page
                    # Search API returns results[0] with 'content' and 'version' at same level
                    result = results['results'][0]
                    page_data = result.get('content', result)  # Fallback to result if no content key

                    # Version might be at result level or content level
                    version_data = result.get('version', page_data.get('version'))

                    if version_data and 'number' in version_data:
                        version_num = version_data['number']
                    else:
                        # Fallback: fetch full page details to get version
                        logging.debug(f"Version not in search results, fetching page details for {page_data['id']}")
                        page_resp = requests.get(
                            url=f'{CONFIG["confluence_url"]}content/{page_data["id"]}',
                            auth=HTTPBasicAuth(login, password),
                            verify=False,
                            timeout=10
                        )
                        if page_resp.status_code == 200:
                            full_page = page_resp.json()
                            version_num = full_page.get('version', {}).get('number', 1)
                        else:
                            version_num = 1  # Default if we can't get version

                    logging.info(f"Found existing page: {page_data['id']} (v{version_num})")
                    return {
                        'id': page_data['id'],
                        'version': version_num,
                        'title': page_data.get('title', '')
                    }
                elif attempt < max_retries - 1:
                    # Not found yet, but we have retries left
                    logging.debug(f"Page not found on attempt {attempt + 1}, will retry...")
                    continue
                else:
                    # Final attempt, page truly doesn't exist
                    logging.debug("No existing page found after all retries")
                    return None
            else:
                logging.warning(f"Search failed with status {response.status_code}")
                if attempt < max_retries - 1:
                    continue
                return None

        except Exception as e:
            logging.warning(f"Error searching for existing page (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                continue
            return None

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

    BULLETPROOF: If creation fails with "title already exists", uses direct
    Content API lookup (bypasses search index) to find and update the page.
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

        # BULLETPROOF FIX: Handle "title already exists" by using direct lookup
        if 'already exists' in error_message.lower() or 'same title' in error_message.lower():
            logging.warning(f"Title exists but search didn't find it - using direct Content API lookup")

            # Use direct Content API lookup (bypasses search index entirely)
            existing_page = findPageByTitleDirect(title, login, password)

            if existing_page:
                logging.info(f"Direct lookup found page {existing_page['id']} - updating instead")
                return updatePage(
                    page_id=existing_page['id'],
                    title=title,
                    content=content,
                    version=existing_page['version'],
                    login=login,
                    password=password
                )
            else:
                # Last resort: wait and retry direct lookup
                import time
                logging.warning("Direct lookup failed, waiting 3s and retrying...")
                time.sleep(3)

                existing_page = findPageByTitleDirect(title, login, password)
                if existing_page:
                    logging.info(f"Retry found page {existing_page['id']} - updating")
                    return updatePage(
                        page_id=existing_page['id'],
                        title=title,
                        content=content,
                        version=existing_page['version'],
                        login=login,
                        password=password
                    )

                logging.error(f"Could not find page even with direct lookup: {title}")

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
# Function for cleanup of orphan pages (differential cleanup)
#
def cleanupOrphanPages(expected_pages_set, login, password):
    """
    Delete autogenerated pages that don't match any local files (orphans).

    This implements differential cleanup: only deletes pages that shouldn't exist,
    leaving all valid pages intact.

    Args:
        expected_pages_set: Set of expected page base titles (from local files)
        login: Confluence email
        password: Confluence API token

    Returns:
        dict with 'deleted_count' and 'orphans' list
    """
    logging.info("Starting differential orphan cleanup...")
    logging.info(f"Expected pages: {len(expected_pages_set)}")

    # Find all autogenerated pages using paginated search
    all_pages = searchPages(login, password)
    logging.info(f"Found {len(all_pages)} autogenerated pages in Confluence")

    if not all_pages:
        logging.info("No pages to clean up")
        return {'deleted_count': 0, 'orphans': []}

    # Identify orphans by comparing against expected pages
    orphan_ids = []
    orphan_details = []

    # Need to fetch page titles to compare
    # Unfortunately, searchPages() only returns IDs, not titles
    # We need to query each page's title or use a different approach

    logging.info("Identifying orphans (this may take a moment)...")

    # Import here to avoid circular dependency
    import requests
    from requests.auth import HTTPBasicAuth

    for page_id in all_pages:
        try:
            # Fetch page details to get title
            response = requests.get(
                url=f'{CONFIG["confluence_url"]}content/{page_id}',
                auth=HTTPBasicAuth(login, password),
                verify=False,
                timeout=10
            )

            if response.status_code == 200:
                page_data = response.json()
                full_title = page_data.get('title', '')

                # Extract base title by removing search pattern
                search_pattern = str(CONFIG["confluence_search_pattern"])
                if search_pattern in full_title:
                    base_title = full_title.replace(f"  {search_pattern}", "").strip()
                else:
                    base_title = full_title

                # Check if this page should exist
                if base_title not in expected_pages_set:
                    orphan_ids.append(page_id)
                    orphan_details.append({
                        'id': page_id,
                        'title': full_title,
                        'base_title': base_title
                    })
                    logging.debug(f"Orphan identified: {base_title} (ID: {page_id})")
                else:
                    logging.debug(f"Valid page: {base_title}")

        except Exception as e:
            logging.warning(f"Error checking page {page_id}: {e}")
            continue

    # Safety check: prevent accidental mass deletion
    orphan_count = len(orphan_ids)
    total_pages = len(all_pages)

    if orphan_count > 0:
        orphan_percentage = (orphan_count / total_pages) * 100
        logging.info(f"Identified {orphan_count} orphan pages ({orphan_percentage:.1f}% of total)")

        # Safety threshold: if >20% would be deleted, require confirmation
        if orphan_percentage > 20:
            logging.warning(f"⚠️  Safety threshold exceeded: {orphan_percentage:.1f}% would be deleted")
            logging.warning(f"   Expected: {len(expected_pages_set)} pages")
            logging.warning(f"   Found: {total_pages} pages")
            logging.warning(f"   Orphans: {orphan_count} pages")
            logging.warning(f"   Skipping cleanup for safety. Review expected pages set.")
            return {
                'deleted_count': 0,
                'orphans': orphan_details,
                'skipped': True,
                'reason': f'Safety threshold exceeded ({orphan_percentage:.1f}% > 20%)'
            }

        # Delete orphans
        logging.info(f"Deleting {orphan_count} orphan pages...")
        deleted = deletePages(orphan_ids, login, password)

        logging.info(f"✅ Cleanup complete: {orphan_count} orphan pages deleted")
        return {
            'deleted_count': orphan_count,
            'orphans': orphan_details,
            'skipped': False
        }
    else:
        logging.info("✅ No orphan pages found - all pages match local files")
        return {'deleted_count': 0, 'orphans': [], 'skipped': False}


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
