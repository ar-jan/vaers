#%%
import logging
import os
import subprocess
import sys

#%%
VAERS_PATH = '../data'
DB = 'vaers.db'
LOGFILE='import.log'

#%%
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOGFILE), logging.StreamHandler(sys.stdout)]
    )
logger = logging.getLogger()

#%%
def import_vaers_csv(filename):
    basename = os.path.splitext(filename)[0]
    tables = ['VAERSDATA', 'VAERSSYMPTOMS', 'VAERSVAX']
    TABLE = next((table for table in tables if basename.endswith(table)), None)
    # Could use sqlite-utils as library instead, but I'm used to the CLI tool.
    command = ['sqlite-utils', 'insert', DB, TABLE, os.path.join(VAERS_PATH, filename), '--csv', '--encoding=iso8859-1', '--detect-types']
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        logger.info(f"{filename} data imported successfully.")
    else:
        logger.warn(f"Importing {filename} failed.")
        logger.error(result.stderr)

#%%
for filename in sorted(os.listdir(VAERS_PATH)):
    import_vaers_csv(filename)
