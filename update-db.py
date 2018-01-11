

import os, time, MySQLdb, hashlib
from os.path import join, getsize
import config

db = MySQLdb.connect(host=config.database["host"],
                    user=config.database["user"],
                    passwd=config.database["passwd"],
                    db=config.database["db"])
cur = db.cursor()

cur.execute("SELECT * FROM files")

rows = cur.fetchall()
deleteCount = 0
addCount = 0

print "FINDING OUTDATED ENTRIES TO REMOVE FROM ZENDIR DB"
for row in rows:
    fullpath = row[1]
    #Check to see if any files were deleted, if so, remove them from the database
    if os.path.isfile(row[1]):
        #check to see if byte, modified date are same
        #currently stored info
        bytes = row[6]
        created = row[9]
        modified = row[10]

        #scanning file directory to compare
        try:
            checkBytes = getsize( row[1] ) # size of this file
        except:
            bytes = 0

        try:
            stats = os.stat( row[1] )
        except OSError, e:
            print("OSError: {}".format(e))

        checkCreated = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(stats.st_ctime))
        checkModified = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(stats.st_mtime))

        if (bytes == checkBytes) & (created == checkCreated) & (modified == checkModified):
            #file hasn't changed, moving on
            pass
        else:
            #we'll delete the entry and it will be added again in a few minutes
            try:
                cur.execute("DELETE FROM files WHERE id=%s LIMIT 1", (row[0], ) )
                db.commit()
            except MySQLdb.Error, e:
                try:
                    print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                except IndexError:
                    deleteCount += 1
                    print "MySQL Error: %s" % str(e)
            print "existing {} does not match db, deleted. This will be added again at the end of the update".format(row[4])

    else:
        #delete this entry from db
        try:
            cur.execute("DELETE FROM files WHERE id=%s LIMIT 1", (row[0], ) )
            db.commit()
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
        deleteCount += 1
        print 'DELETED:"{}" (no longer exists)'.format(row[4])

cur.close()
print "TOTAL:"
print "------"
print "Removed {} outdated db entries".format(deleteCount)



print "\nSCANNING FOR NEW FILES\n"
cur = db.cursor()

rootpath = config.directoryToAnalyze
print "Scanning files at ", rootpath

for dirpath, dirs, files in os.walk(rootpath):
    for filename in files:
        filepath = join(dirpath, filename)
        #import pdb; pdb.set_trace()

        query = "SELECT COUNT(1) FROM files WHERE fullpath = %s"
        field_inserts = (filepath, )

        try:
            cur.execute(query, field_inserts)
            db.commit()
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)

        dbResult = cur.fetchone()[0]

        if dbResult:
            #file already exists in db so skip it
            pass

        else:
            print "Found new file at %s" % filepath

            # get the file extension
            # use os.path.splitext() to split the filename on the last period
            # returns an array of two items; take the second by doing [1]
            # returned string will have the period on the front; strip it with [1:]
            # make it all lowercase
            extension = os.path.splitext(filename)[1][1:].lower()

            if extension == "jpg":
                extension = "jpeg"

            try:
                bytes = getsize( filepath ) # size of this file
            except:
                bytes = 0
                # FIXME: This exception handling was added due to the error:
                # "OSError: [Errno 11] Resource temporarily unavailable"
                # Perhaps have some handling for that, or consider adding a flag
                # to the line in the database for this items saying "err: bytes"
                # or something like that.

            # path to files with top level removed
            # this will make it easier to translate between S-drive files being
            # analyzed on a computer other than JSC-MOD-FS3
            relativepath = filepath[len(rootpath):]

            try:
                stats = os.stat( filepath )
            except OSError, e:
                print("OSError: {}".format(e))

            created = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(stats.st_ctime))
            modified = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(stats.st_mtime))
            accessed = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(stats.st_atime))

            query = """
                INSERT INTO files
                (fullpath,rootpath,relativepath,filename,extension,bytes,created,modified,accessed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            field_inserts = (filepath,rootpath,relativepath,filename,extension,bytes,created,modified,accessed)

            try:
                cur.execute(query, field_inserts)
                db.commit()
            except MySQLdb.Error, e:
                try:
                    print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                except IndexError:
                    print "MySQL Error: %s" % str(e)
            addCount += 1

    print "Complete with directory", dirpath

cur.close()

#
# This needs to be done after the files have been scanned
#
print "\nBUILDING DIRECTORIES DATABASE\n"
cur = db.cursor() # make a new one...
#wipe all data from current table
cur.execute("DELETE FROM directories")
db.commit()

cur.execute("""
SELECT relativepath FROM files
""")

rows = cur.fetchall()

dirs = {}

for row in rows:
    dir = row[0][0:row[0].rfind('/')]
    if dir in dirs:
        dirs[dir] += 1
    else:
        print "ADDED DIRECTORY: {0}".format(dir)
        dirs[dir] = 1

print "\nCOMPLETE SCAN, START INSERTING DIRECTORY DATA\n"
totalDirs = len(dirs)
dirCount = 0
for dir in dirs:
    if dir == "":
        dir = "<root>"

    dirCount += 1
    print "{} ({}/{})".format(dir, dirCount, totalDirs)

    try:
        cur.execute( "INSERT INTO directories (path) VALUES ( %(dir)s )", { 'dir': dir } )
        db.commit()
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            print "MySQL Error: %s" % str(e)

cur.close()

print "complete generating directory listings"
print "{} files removed".format(deleteCount)
print "{} files added".format(addCount)




