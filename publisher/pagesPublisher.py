import logging
import os
import markdown
import re
from config.getconfig import getConfig
from pagesController import createPage
from pagesController import attachFile


CONFIG = getConfig()

# Global tracking collections
publish_errors = []
success_count = 0
created_count = 0
updated_count = 0


def buildExpectedPagesSet(folder):
    """
    Build set of expected page titles from local markdown files and directories.

    Walks the entire directory tree and collects relative paths (from base folder)
    for both directories and .md files. This ensures unique titles in Confluence.

    Returns: Set of page titles using relative paths (without search pattern suffix)
    """
    expected_pages = set()
    base_folder = os.path.abspath(folder)

    logging.info(f"Building expected pages set from: {folder}")

    for root, dirs, files in os.walk(folder):
        # Calculate relative path from base folder for this directory
        rel_root = os.path.relpath(root, base_folder)

        # Add directory names with relative paths
        for dir_name in dirs:
            if rel_root == '.':
                # Top-level directory
                rel_path = dir_name
            else:
                # Nested directory
                rel_path = os.path.join(rel_root, dir_name)
            # Normalize path separators to forward slashes for consistency
            rel_path = rel_path.replace(os.sep, '/')
            expected_pages.add(rel_path)
            logging.debug(f"Expected directory page: {rel_path}")

        # Add .md filenames with relative paths
        for filename in files:
            if filename.lower().endswith('.md'):
                if rel_root == '.':
                    # Top-level file
                    rel_path = filename
                else:
                    # Nested file
                    rel_path = os.path.join(rel_root, filename)
                # Normalize path separators to forward slashes for consistency
                rel_path = rel_path.replace(os.sep, '/')
                expected_pages.add(rel_path)
                logging.debug(f"Expected file page: {rel_path}")

    logging.info(f"Built expected pages set: {len(expected_pages)} pages")
    return expected_pages

def publishFolder(folder, login, password, parentPageID = None, base_folder = None):
    """
    Recursively publish folders and files to Confluence.

    Args:
        folder: Current folder being processed
        login: Confluence email
        password: Confluence API token
        parentPageID: Parent page ID in Confluence (None for root)
        base_folder: Base folder for calculating relative paths (for unique titles)
    """
    global publish_errors, success_count, created_count, updated_count

    # On first call, set base_folder to the initial folder
    if base_folder is None:
        base_folder = os.path.abspath(folder)

    logging.info("Publishing folder: " + folder)

    for entry in os.scandir(folder):
        if entry.is_dir():
            # Calculate unique title using relative path from base folder
            rel_path = os.path.relpath(entry.path, base_folder)
            rel_path = rel_path.replace(os.sep, '/')  # Normalize to forward slashes

            # create page with the DISPLAY CHILDREN macro for the directories in the folder with MD files
            logging.info("Found directory: " + str(entry.path))
            result = createPage(title=rel_path,
                content="<ac:structured-macro ac:name=\"children\" ac:schema-version=\"2\" ac:macro-id=\"80b8c33e-cc87-4987-8f88-dd36ee991b15\"/>", # name of the DISPLAY CHILDREN macro
                parentPageID = parentPageID,
                login=login,
                password=password)

            # Check if page creation was successful
            if result['success']:
                success_count += 1
                if result.get('operation') == 'created':
                    created_count += 1
                elif result.get('operation') == 'updated':
                    updated_count += 1
                currentPageID = result['page_id']
                # publish files in the current folder (pass base_folder through)
                publishFolder(folder=entry.path, login=login, password=password, parentPageID=currentPageID, base_folder=base_folder)
            else:
                # Log error and continue processing
                error_info = {
                    'path': str(entry.path),
                    'type': 'directory',
                    'error': result.get('error', 'Unknown error'),
                    'status_code': result.get('status_code', 'N/A')
                }
                publish_errors.append(error_info)
                logging.warning(f"Skipping directory {entry.path} and its children due to error")
                # Don't recurse into this directory since we couldn't create the parent page
            
        elif entry.is_file():
            logging.info("Found file: " + str(entry.path))

            if str(entry.path).lower().endswith('.md'): # chech for correct file extension
                # Calculate unique title using relative path from base folder
                rel_path = os.path.relpath(entry.path, base_folder)
                rel_path = rel_path.replace(os.sep, '/')  # Normalize to forward slashes

                newFileContent = ""
                filesToUpload = []
                with open(entry.path, 'r', encoding="utf-8") as mdFile:
                    for line in mdFile:

                        # search for images in each line and ignore http/https image links
                        # Pattern: \A!\[.*]\(.*\)\Z
                        # example:  ![test](/data_images/test_image.jpg)

                        result = re.findall("\A!\[.*]\((?!http)(.*)\)", line)

                        if bool(result):   # line contains an image
                            # extract filename from the full path
                            result = str(result).split('\'')[1]  # ['/data_images/test_image.jpg'] => /data_images/test_image.jpg
                            result = str(result).split('/')[-1]  # /data_images/test_image.jpg => test_image.jpg
                            logging.debug("Found file for attaching: " + result)
                            filesToUpload.append(result)
                            # replace line with conflunce storage format <ac:image> <ri:attachment ri:filename="test_image.jpg" /></ac:image>
                            newFileContent += "<ac:image> <ri:attachment ri:filename=\"" + result + "\" /></ac:image>"
                        else:  # line without an image
                            newFileContent += line

                    # create new page with unique title (relative path)
                    result = createPage(title=rel_path,
                        content=markdown.markdown(newFileContent, extensions=['markdown.extensions.tables', 'fenced_code']),
                        parentPageID = parentPageID,
                        login=login,
                        password=password)

                    # Check if page creation was successful
                    if result['success']:
                        success_count += 1
                        if result.get('operation') == 'created':
                            created_count += 1
                        elif result.get('operation') == 'updated':
                            updated_count += 1
                        pageIDforFileAttaching = result['page_id']

                        # if do exist files to Upload as attachments
                        if bool(filesToUpload):
                            for file in filesToUpload:
                                imagePath = str(CONFIG["github_folder_with_image_files"]) + "/" + file #full path of uploaded image file
                                if os.path.isfile(imagePath): # check if the  file exist
                                    logging.info("Attaching file: " + imagePath + "  to the page: " + str(pageIDforFileAttaching))
                                    with open(imagePath, 'rb') as attachedFile:
                                        attachFile(pageIdForFileAttaching=pageIDforFileAttaching,
                                            attachedFile=attachedFile,
                                            login=login,
                                            password=password)
                                else:
                                    logging.error("File: " + str(imagePath) + "  not found. Nothing to attach")
                    else:
                        # Log error and continue processing
                        error_info = {
                            'path': str(entry.path),
                            'type': 'file',
                            'error': result.get('error', 'Unknown error'),
                            'status_code': result.get('status_code', 'N/A')
                        }
                        publish_errors.append(error_info)
                        logging.warning(f"Skipping file {entry.path} due to error")
            else:
                logging.info("File: " + str(entry.path) + "  is not a MD file. Publishing has rejected")

        elif entry.is_symlink():
            logging.info("Found symlink: " + str(entry.path))

        else:
            logging.info("Found unknown type of entry (not file, not directory, not symlink) " + str(entry.path))
