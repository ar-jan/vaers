#%%
import logging
import os
import subprocess
import sqlite3
import sys

from sqlite_utils import Database

#%%
VAERS_PATH = '../data'
DB = 'vaers.db'
LOGFILE='import.log'
db = Database(DB)

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

#%%
# Add normalized symptoms table. Note: VAERS_ID appears to be repeated
# for listing more than 5 symptoms for the same VAERS_ID.
db['VAERSSYMPTOMS_NORM'].create(
    {
        "VAERS_ID": int,
        "SYMPTOM": str,
        "SYMPTOMVERSION": float,
    }, if_not_exists=True
)

#%%
# Insert unpivoted symptoms data
cur = db.executescript("""
INSERT INTO VAERSSYMPTOMS_NORM
SELECT VAERS_ID, SYMPTOM1 AS SYMPTOM, SYMPTOMVERSION1 AS SYMPTOMVERSION
FROM VAERSSYMPTOMS
WHERE SYMPTOM1 IS NOT NULL AND SYMPTOM1 != ''
UNION ALL
SELECT VAERS_ID, SYMPTOM2 AS SYMPTOM, SYMPTOMVERSION2 AS SYMPTOMVERSION
FROM VAERSSYMPTOMS
WHERE SYMPTOM2 IS NOT NULL AND SYMPTOM2 != ''
UNION ALL
SELECT VAERS_ID, SYMPTOM3 AS SYMPTOM, SYMPTOMVERSION3 AS SYMPTOMVERSION
FROM VAERSSYMPTOMS
WHERE SYMPTOM3 IS NOT NULL AND SYMPTOM3 != ''
UNION ALL
SELECT VAERS_ID, SYMPTOM4 AS SYMPTOM, SYMPTOMVERSION4 AS SYMPTOMVERSION
FROM VAERSSYMPTOMS
WHERE SYMPTOM4 IS NOT NULL AND SYMPTOM4 != ''
UNION ALL
SELECT VAERS_ID, SYMPTOM5 AS SYMPTOM, SYMPTOMVERSION5 AS SYMPTOMVERSION
FROM VAERSSYMPTOMS
WHERE SYMPTOM5 IS NOT NULL AND SYMPTOM5 != '';
""")

#%%
# Add primary key
db['VAERSDATA'].transform(pk='VAERS_ID')

#%%
db.executescript("""
CREATE INDEX VAERSSYMPTOMS_NORM_VAERS_ID_IDX ON VAERSSYMPTOMS_NORM (VAERS_ID);
CREATE INDEX VAERSSYMPTOMS_NORM_SYMPTOM_IDX ON VAERSSYMPTOMS_NORM (SYMPTOM);
CREATE INDEX VAERSVAX_VAERS_ID_IDX ON VAERSVAX (VAERS_ID);
CREATE INDEX VAERSVAX_VAX_TYPE_IDX ON VAERSVAX (VAX_TYPE);
CREATE INDEX VAERSVAX_VAX_NAME_IDX ON VAERSVAX (VAX_NAME);
""")

#%%
db.close()
