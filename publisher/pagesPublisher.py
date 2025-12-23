import logging
import os
import markdown
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.getconfig import getConfig
from pagesController import createPage
from pagesController import attachFile


CONFIG = getConfig()


class PublishStats:
    """Thread-safe statistics tracking for parallel publishing."""

    def __init__(self):
        self.lock = threading.Lock()
        self.errors = []
        self.success_count = 0
        self.created_count = 0
        self.updated_count = 0

    def add_success(self, operation=None):
        with self.lock:
            self.success_count += 1
            if operation == 'created':
                self.created_count += 1
            elif operation == 'updated':
                self.updated_count += 1

    def add_error(self, error_info):
        with self.lock:
            self.errors.append(error_info)


# Create global instance
_stats = PublishStats()

# Legacy global variables for backward compatibility
publish_errors = _stats.errors
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


def processMarkdownFile(file_entry, parentPageID, login, password, base_folder):
    """
    Process a single markdown file and create Confluence page.

    This function is extracted to enable parallel file processing.
    Thread-safe: No shared state between files.
    """
    rel_path = os.path.relpath(file_entry.path, base_folder)
    rel_path = rel_path.replace(os.sep, '/')

    logging.info("Processing file: " + str(file_entry.path))

    newFileContent = ""
    filesToUpload = []

    with open(file_entry.path, 'r', encoding="utf-8") as mdFile:
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

    # Create new page with unique title (relative path)
    result = createPage(
        title=rel_path,
        content=markdown.markdown(newFileContent, extensions=['markdown.extensions.tables', 'fenced_code']),
        parentPageID=parentPageID,
        login=login,
        password=password
    )

    # Track result
    if result['success']:
        _stats.add_success(operation=result.get('operation'))
        pageID = result['page_id']

        # if do exist files to Upload as attachments
        if bool(filesToUpload):
            for file in filesToUpload:
                imagePath = str(CONFIG["github_folder_with_image_files"]) + "/" + file  # full path of uploaded image file
                if os.path.isfile(imagePath):  # check if the  file exist
                    logging.info("Attaching file: " + imagePath + "  to the page: " + str(pageID))
                    with open(imagePath, 'rb') as attachedFile:
                        attachFile(
                            pageIdForFileAttaching=pageID,
                            attachedFile=attachedFile,
                            login=login,
                            password=password
                        )
                else:
                    logging.error("File: " + str(imagePath) + "  not found. Nothing to attach")
    else:
        # Log error and continue processing
        error_info = {
            'path': str(file_entry.path),
            'type': 'file',
            'error': result.get('error', 'Unknown error'),
            'status_code': result.get('status_code', 'N/A')
        }
        _stats.add_error(error_info)
        logging.warning(f"Skipping file {file_entry.path} due to error")

    return result


def publishFolder(folder, login, password, parentPageID=None, base_folder=None, executor=None):
    """
    Recursively publish folders and files to Confluence.

    Directories are processed SEQUENTIALLY (to avoid thread pool deadlock).
    Files within each directory are processed in PARALLEL for speedup.

    Args:
        folder: Current folder being processed
        login: Confluence email
        password: Confluence API token
        parentPageID: Parent page ID in Confluence (None for root)
        base_folder: Base folder for calculating relative paths (for unique titles)
        executor: ThreadPoolExecutor for parallel file processing (created on first call)
    """
    # Initialize base_folder on first call
    if base_folder is None:
        base_folder = os.path.abspath(folder)

    # Create executor on first call (shared across all recursive calls)
    is_root = executor is None
    if is_root:
        executor = ThreadPoolExecutor(max_workers=4)  # For parallel file processing
        logging.info("Initialized parallel executor with 4 workers for file processing")

    try:
        logging.info("Publishing folder: " + folder)

        # Collect entries
        dirs = []
        files = []
        for entry in os.scandir(folder):
            if entry.is_dir():
                dirs.append(entry)
            elif entry.is_file() and str(entry.path).lower().endswith('.md'):
                files.append(entry)

        # PHASE 1: Process all subdirectories SEQUENTIALLY (avoids deadlock)
        # Each directory page must be created before its children can be processed
        for dir_entry in dirs:
            # Calculate unique title
            rel_path = os.path.relpath(dir_entry.path, base_folder)
            rel_path = rel_path.replace(os.sep, '/')

            # Create directory page
            logging.info(f"Creating directory page: {rel_path}")
            result = createPage(
                title=rel_path,
                content="<ac:structured-macro ac:name=\"children\" ac:schema-version=\"2\" ac:macro-id=\"80b8c33e-cc87-4987-8f88-dd36ee991b15\"/>",
                parentPageID=parentPageID,
                login=login,
                password=password
            )

            if result['success']:
                _stats.add_success(operation=result.get('operation'))
                currentPageID = result['page_id']

                # Recursively process subdirectory (SEQUENTIAL - no thread pool for directories)
                publishFolder(
                    folder=dir_entry.path,
                    login=login,
                    password=password,
                    parentPageID=currentPageID,
                    base_folder=base_folder,
                    executor=executor  # Pass executor for file processing
                )
            else:
                _stats.add_error({
                    'path': str(dir_entry.path),
                    'type': 'directory',
                    'error': result.get('error', 'Unknown error'),
                    'status_code': result.get('status_code', 'N/A')
                })
                logging.warning(f"Skipping directory {dir_entry.path} and its children due to error")

        # PHASE 2: Process all files in PARALLEL (main speedup)
        if files:
            logging.info(f"Processing {len(files)} files in parallel...")
            file_futures = {}

            for file_entry in files:
                future = executor.submit(
                    processMarkdownFile,
                    file_entry=file_entry,
                    parentPageID=parentPageID,
                    login=login,
                    password=password,
                    base_folder=base_folder
                )
                file_futures[future] = file_entry.path

            # Wait for all file processing in this folder to complete
            for future in as_completed(file_futures):
                path = file_futures[future]
                try:
                    future.result()
                    logging.debug(f"Completed file: {path}")
                except Exception as e:
                    logging.error(f"Error processing file {path}: {e}")

    finally:
        # Shutdown executor only at root level
        if is_root:
            executor.shutdown(wait=True)
            logging.info("Parallel executor shutdown complete")
