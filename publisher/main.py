import argparse
import logging
import sys

from config.getconfig import getConfig
from pagesController import cleanupOrphanPages
from pagesPublisher import publishFolder, buildExpectedPagesSet, publish_errors, success_count, created_count, updated_count

logging.basicConfig(level=logging.INFO)


# Parse arguments with LOGIN and PASSWORD for Confluence
parser = argparse.ArgumentParser()
parser.add_argument('--login', help='Login with "" is mandatory', required=True)
parser.add_argument('--password', help='Password with "" is mandatory',  required=True)
args = parser.parse_args()
inputArguments = vars(args)


CONFIG = getConfig()

logging.debug(CONFIG)

# TEMPORARY: Clean slate migration - delete all old pages with old titles
# This will be removed after one run
logging.info("=" * 80)
logging.info("MIGRATION: Deleting all pages with old title format")
logging.info("=" * 80)
from pagesController import searchPages, deletePages
pages = searchPages(login=inputArguments['login'], password=inputArguments['password'])
deletePages(pagesIDList=pages, login=inputArguments['login'], password=inputArguments['password'])
logging.info(f"Deleted {len(pages)} pages")

# Skip publishing on this run to avoid conflicts
logging.info("\n⚠️  Clean slate complete - skipping publish")
logging.info("   Remove DELETE code and re-run to publish with new titles")
sys.exit(0)

# Step 3: Cleanup orphan pages (only if publish was successful)
logging.info("\n" + "=" * 80)
logging.info("PHASE 3: Differential cleanup (orphan pages)")
logging.info("=" * 80)

if len(publish_errors) == 0:
    cleanup_result = cleanupOrphanPages(
        expected_pages_set=expected_pages,
        login=inputArguments['login'],
        password=inputArguments['password']
    )

    if cleanup_result.get('skipped'):
        logging.warning(f"⚠️  Cleanup skipped: {cleanup_result.get('reason')}")
    else:
        logging.info(f"✅ Cleanup complete: {cleanup_result['deleted_count']} orphans removed")
else:
    logging.warning("⚠️  Skipping cleanup due to publishing errors")
    logging.warning("   Fix errors and re-run to clean up orphans")

# Print summary report
print("\n" + "="*80)
print("CONFLUENCE PUBLISHING SUMMARY")
print("="*80)
print(f"\n✅ SUCCESSFUL: {success_count} pages published")
print(f"   📝 Created: {created_count} new pages")
print(f"   🔄 Updated: {updated_count} existing pages")
print(f"❌ FAILED: {len(publish_errors)} pages")

# Calculate success rate
total_attempted = success_count + len(publish_errors)
if total_attempted > 0:
    success_rate = (success_count / total_attempted) * 100
    print(f"\n📊 Success Rate: {success_rate:.1f}%")

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
