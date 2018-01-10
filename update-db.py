#If file is does not exist in database then add it as normal
#all of these processes will need a new column for total path.

import os, time, MySQLdb, hashlib
from os.path import join, getsize
import config

db = MySQLdb.connect(host=config.database["host"],
                    user=config.database["user"],
                    passwd=config.database["passwd"],
                    db=config.database["db"])
cur = db.cursor()

cur.execute("""
SELECT * FROM files
""")

rows = cur.fetchall()

#Check to see if any files were deleted, if so, remove them from the database

#Scan through all files, check files found to see if the filepath, name, bytes and last modified are all the same. If they are then skip this file without doing anything to the database
for row in rows:
    fullpath = row[1]
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
            pass
            print "this would have been deleted due descripency between dates or size"

    else:
        #delete this entry from db
        pass
        print "this would have been deleted due to it not exisitng"

print "Removed outdated db entries"
# Close communication with the database. May be required prior to next part
cur.close()

print "Updating database..."
cur = db.cursor() # make a new one...

rootpath = config.directoryToAnalyze
print "Scanning files at ", rootpath

for dirpath, dirs, files in os.walk(rootpath):
    for filename in files:
        filepath = join(dirpath, filename)
        #import pdb; pdb.set_trace()

        query = """
            SELECT COUNT(1) FROM files
            WHERE fullpath = %s
            """

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
            print "This already exists so we're skipping"
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
            owner = stats.st_uid

            query = """
                INSERT INTO files
                (fullpath,rootpath,relativepath,filename,extension,bytes,created,modified,accessed, owner)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            field_inserts = (filepath,rootpath,relativepath,filename,extension,bytes,created,modified,accessed, owner)

            print query % field_inserts

            try:
                cur.execute(query, field_inserts)
                db.commit()
            except MySQLdb.Error, e:
                try:
                    print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                except IndexError:
                    print "MySQL Error: %s" % str(e)


        print "Complete with directory", dirpath

    # Close communication with the database. May be required prior to next part
cur.close()


#
# This needs to be done after the files have been scanned
#
print "generating directories..."
cur = db.cursor() # make a new one...

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
        print "NEW directory: {0}".format(dir)
        dirs[dir] = 1

print "\nCOMPLETE SCAN, START INSERT\n"

for dir in dirs:
    if dir == "":
        dir = "<root>"
    # else:
    #   dir = MySQLdb.escape_string(dir)
    print dir
    print "INSERTING dir: {0}".format(dir)
    try:
        #this needs to be worked on it, it isn't actually working
        cur.execute( "INSERT INTO directories (path) VALUES ( %(dir)s ) ON DUPLICATE KEY UPDATE path ", { 'dir': dir } )
        db.commit()
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            print "MySQL Error: %s" % str(e)

# Close communication with the database
cur.close()

print "complete generating directory listings"



