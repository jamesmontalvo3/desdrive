import os, time, MySQLdb, hashlib
from os.path import join, getsize
import config

db = MySQLdb.connect(host=config.database["host"],
                    user=config.database["user"],
                    passwd=config.database["passwd"],
                    db=config.database["db"])
cur = db.cursor()

### make a list of all hashes

cur.execute("""
SELECT DISTINCT sha1 FROM files
""")

hashes = cur.fetchall()
hashlist = []
for hashnum in hashes:
    hashlist += hashnum

#### Query for each hash
for hashnum in hashlist:
    t = (hashnum, )

##Check if there are dupes
    cur.execute('SELECT COUNT(*) FROM files WHERE sha1=%s', t)
    numDupes = cur.fetchone()

## if there is more than one result then keep it, order with most recently accessed on top
    if numDupes[0] > 1:
        cur.execute('SELECT rootpath, relativepath, filename, modified, accessed FROM files WHERE sha1=%s ORDER BY accessed ASC', t)
        results = cur.fetchall()

        count = 1

        for result in results:

            if count == 1:
                print("""
Filename: {}
Total # Duplicates {}
Last Accessed: {}
Reccomend Keeping Location: {}
-------------------------------------------
Additional Duplicates
""".format(result[2], numDupes[0], result[4], result[1])
)
                count += 1
            else:
                print(' {}.  Location: {}{}'.format(count, result[1], result[2]))
                count += 1

