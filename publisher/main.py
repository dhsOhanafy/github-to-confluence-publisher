import argparse
import logging
import sys

from config.getconfig import getConfig
from pagesController import deletePages, searchPages
from pagesPublisher import publishFolder, publish_errors, success_count

logging.basicConfig(level=logging.INFO)


# Parse arguments with LOGIN and PASSWORD for Confluence
parser = argparse.ArgumentParser()
parser.add_argument('--login', help='Login with "" is mandatory', required=True)
parser.add_argument('--password', help='Password with "" is mandatory',  required=True)
args = parser.parse_args()
inputArguments = vars(args)


CONFIG = getConfig()

logging.debug(CONFIG)

pages = searchPages(login=inputArguments['login'], password=inputArguments['password'])
deletePages(pagesIDList=pages, login=inputArguments['login'], password=inputArguments['password'])

publishFolder(folder = str(CONFIG["github_folder_with_md_files"]),
  login=inputArguments['login'],
  password=inputArguments['password'])

# Print summary report
print("\n" + "="*80)
print("CONFLUENCE PUBLISHING SUMMARY")
print("="*80)
print(f"\n✅ SUCCESSFUL: {success_count} pages created/updated")
print(f"❌ FAILED: {len(publish_errors)} pages")

if publish_errors:
    print("\nERRORS:")
    print("-" * 80)
    for idx, error in enumerate(publish_errors, 1):
        print(f"\n{idx}. {error['path']}")
        print(f"   Type: {error['type']}")
        print(f"   Error: {error['error']}")
        print(f"   Status Code: {error['status_code']}")

    print("\n" + "-" * 80)
    print("RECOMMENDATION:")
    print("- Review the errors above and fix the issues")
    print("- Re-run the workflow to publish remaining changes")
    print("="*80 + "\n")
    sys.exit(1)  # Exit with error code if any failures
else:
    print("\n🎉 All pages published successfully!")
    print("="*80 + "\n")
    sys.exit(0)  # Exit successfully
