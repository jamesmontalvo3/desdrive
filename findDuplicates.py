import os, time, MySQLdb, hashlib
from os.path import join, getsize
import config

db = MySQLdb.connect(host=config.database["host"],
                    user=config.database["user"],
                    passwd=config.database["passwd"],
                    db=config.database["db"])
cur = db.cursor()

cur.execute("""
SELECT * FROM files WHERE sha1 IS NULL
""")

rows = cur.fetchall()

for row in rows:
    filepath = row[2] + row[3]
    print filepath
    # Can't sha1 if you can't access the file

    try:
        bytes = getsize( filepath ) # size of this file
    except:
        bytes = 0

    try:
        sha1 = hashlib.sha1()
        sha1.update( file( filepath , 'rb').read() )
        sha1 = sha1.hexdigest()
    except:
        sha1 = "unable-to-generate-sha1"

    query = """
        UPDATE files
        SET sha1=%s, bytes=%s
        WHERE id=%s
    """

    field_inserts = (sha1, bytes, row[0])

    print query % field_inserts

    try:
        cur.execute(query, field_inserts)
        db.commit()
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            print "MySQL Error: %s" % str(e)

    print "Hashes created"

# Close communication with the database. May be required prior to next part
cur.close()

cur = db.cursor() #generate a new one, just in case

print "Populate is_dupe column"

# mark all files as not duplicate
cur.execute("UPDATE files SET is_dupe=0")

# find the actual duplicates
cur.execute("""
SELECT
    sha1,
    COUNT(*) AS num_duplicates
FROM files
WHERE
    sha1 != ""
    AND sha1 IS NOT NULL
    AND bytes != 0
GROUP BY sha1
HAVING num_duplicates > 1
ORDER BY num_duplicates DESC
""")

rows = cur.fetchall()

numrows = len(rows)
for i, row in enumerate(rows):
    print "{0} of {1} Setting {2} as duplicate".format(i+1, numrows, row[0])
    try:
        cur.execute( "UPDATE files SET is_dupe=1 WHERE sha1=%(sha1)s", { 'sha1': row[0] } )
        db.commit()
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            print "MySQL Error: %s" % str(e)


# Close communication with the database
cur.close()

print "Complete marking duplicates"

#
# Mark directory duplicates
#
print "Mark directory info (duplicates? quantities?)"
cur = db.cursor() #again, just to be sure, recreate

cur.execute("SELECT path FROM directories")

dirs = cur.fetchall()
numDirs = len(dirs)

for i,dir in enumerate(dirs):
    dirpath = dir[0]
    print "{0} of {1} RECORDING {2}".format(i, numDirs, dirpath)
    cur.execute("""
            SELECT
                COUNT(*), SUM(is_dupe), SUM(bytes), SUM( IF(is_dupe,bytes,0) )
            FROM files
            WHERE
                relativepath LIKE "{0}%"
        """.format( db.escape_string(dirpath) ) )

    numFiles = cur.fetchone()

    cur.execute( "UPDATE directories SET num_files=%s, num_dupes=%s, total_bytes=%s, dupe_bytes=%s WHERE path=%s",
            (numFiles[0], numFiles[1], numFiles[2], numFiles[3], dirpath) )
    db.commit()

print "File quantities per directory are populated"
