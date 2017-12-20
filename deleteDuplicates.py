import csv, os, sys, time, MySQLdb, hashlib
from os.path import join, getsize
import config

db = MySQLdb.connect(host=config.database["host"],
                    user=config.database["user"],
                    passwd=config.database["passwd"],
                    db=config.database["db"])
cur = db.cursor()
totalbytesremoved = 0
totalremovedfiles = 0

#Create table to record files removed and where the remaining instance of the file still remains
try:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS removedDuplicates
    (
      id int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
      removedFile varchar(255) binary NOT NULL,
      removedFrom varchar(255) binary NOT NULL,
      remiainingLocation varchar(255) binary NOT NULL
    );
    """
    )

except (MySQLdb.Error, MySQLdb.Warning) as e:
    print(e)

#Pull all distince shas from scanned drive and create a list of them
cur.execute("""
SELECT DISTINCT sha1 FROM files
""")

hashes = cur.fetchall()
hashlist = []
for hashnum in hashes:
    hashlist += hashnum

#Query for each hash
for hashnum in hashlist:
    t = (hashnum, )

#Check if there are duplicates with that hash
    cur.execute('SELECT COUNT(*) FROM files WHERE sha1=%s', t)
    numDupes = cur.fetchone()

#If there is more than one then query for all duplicates of said has and order them by date accessed (most recent first)
    if numDupes[0] > 1:
        print("Found dupicates for: {}!".format(hashnum))
        cur.execute('SELECT rootpath, relativepath, filename, modified, accessed, bytes FROM files WHERE sha1=%s ORDER BY accessed ASC', t)
        results = cur.fetchall()

        #counter to be verify only most recently accessed of duplicate files is kept
        count = 1
        duplicateLocations = ''
        filesize = 0
        removedfilecount = 0
        headers = ['File', 'Moved to']

        for result in results:
            if count == 1:
                primeFileLocation = "{}{}".format(result[0], result[1])
                count += 1
                print("*Keeping: {}".format(primeFileLocation))
            else:
                filepath = "{}{}".format(result[0], result[1])
                currentFolder = (result[1][:result[1].rfind('/')])
                filesize += result[5]
                log = "{}{}/MOVED_FILES.csv".format(result[0], currentFolder)

                query = """
                INSERT INTO removedDuplicates
                (removedFile, removedFrom, remiainingLocation)
                VALUES (%s, %s, %s)
                """

                field_inserts = (result[2], filepath, primeFileLocation)

                #Build database of files we removed and where the remaining file exists
                try:
                    cur.execute(query, field_inserts)
                    db.commit()
                except MySQLdb.Error, e:
                    try:
                        print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                    except IndexError:
                        print "MySQL Error: %s" % str(e)

                #Time to actually delete the duplicate
                try:
                    os.remove(filepath)
                except OSError as e:
                    print(e)
                    #Let's not continue with script if our db is out of date
                    sys.exit("\n!!!A file in your 'files' database could not be found for deletion. Re-run 'scan.py' to update your database before proceeding!!!!\n")

                removedfilecount += 1

                #Add MOVED_FILES to folder with file path to kept file. If MOVED_FILES already exists then just append to it.
                if os.path.isfile(log):
                    try:
                        with open(log, "ab") as myfile:
                            logwriter = csv.writer(myfile)
                            logwriter.writerow([result[2], primeFileLocation])
                    except IOError as e:
                        print(e)
                    print("*Deleted: {} (Adding to folder's breadcrumbs)".format(filepath))

                else:
                    try:
                        with open(log, "wb") as myfile:
                            logwriter = csv.writer(myfile)
                            logwriter.writerow(headers)
                            logwriter.writerow([result[2], primeFileLocation])
                    except IOError as e:
                        print(e)
                    print("*Deleted: {} (Creating breadcrumbs)".format(filepath))


                count += 1

        #Print out what was deleted
        totalbytesremoved += filesize
        gbsremoved = totalbytesremoved/1000000000
        totalremovedfiles += removedfilecount
        print("Removed {} duplicates for {}\n".format(removedfilecount, hashnum))

print('All Duplicates Removed!!')
print('Freed up {} bytes ({} Gigabytes) of space!'.format(totalbytesremoved, gbsremoved))

