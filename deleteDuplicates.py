import csv, os, time, MySQLdb, hashlib
from os.path import join, getsize
import config

db = MySQLdb.connect(host=config.database["host"],
                    user=config.database["user"],
                    passwd=config.database["passwd"],
                    db=config.database["db"])
cur = db.cursor()
dupwriter = csv.writer(open('duplicateSummary.csv', 'w'))
header = ['File', 'File Size (bytes)', '# of Duplicates', 'Duplicate Location(s)', 'Comments']
dupwriter.writerow(header)

### make a list of all hashes

cur.execute("""
CREATE TABLE removedDuplicates
(
  id int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
  removedFile varchar(255) binary NOT NULL,
  removedFrom varchar(255) binary NOT NULL,
  remiainingLocation varchar(255) binary NOT NULL
);
"""
)

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
        cur.execute('SELECT rootpath, relativepath, filename, modified, accessed, bytes FROM files WHERE sha1=%s ORDER BY accessed ASC', t)
        results = cur.fetchall()

        count = 1

        duplicateLocations = ''

        for result in results:

            if count == 1:
                primeFile = ("""Filename: {}
Last Accessed: {}
Location: {}
""".format(result[2], result[4], result[1])
)
                primeFileLocation = "{}{}".format(result[0], result[1])
                count += 1
                print(primeFile)
            else:
                filepath = "{}{}".format(result[0], result[1])
                currentFolder = (result[1][:result[1].rfind('/')])
                log = "{}{}/movedFiles.txt".format(result[0], currentFolder)
                print("LOG LOCATION:{}".format(log))

                if os.path.isfile(log):
                    print("log exists")
                    with open(log, "ab") as myfile:
                        myfile.write("File:{} Moved to:{}\n".format(result[2], primeFileLocation))
                else:
                    print("creating log")
                    with open(log, "wb") as myfile:
                        myfile.write("File:{} Moved to:{}\n".format(result[2], primeFileLocation))

                query = """
                INSERT INTO removedDuplicates
                (removedFile, removedFrom, remiainingLocation)
                VALUES (%s, %s, %s)
                """

                field_inserts = (result[2], filepath, primeFileLocation)

                print query % field_inserts

                try:
                    cur.execute(query, field_inserts)
                    db.commit()
                except MySQLdb.Error, e:
                    try:
                        print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                    except IndexError:
                        print "MySQL Error: %s" % str(e)

                print(filepath)
                os.remove(filepath)
                count += 1

print('Duplicates Removed')

