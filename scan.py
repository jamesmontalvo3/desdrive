#!/usr/bin/env python

import os, time, MySQLdb, hashlib
from os.path import join, getsize
import config

db = MySQLdb.connect(host=config.database["host"],
					user=config.database["user"],
					passwd=config.database["passwd"],
					db=config.database["db"])

def clear():

    """Clears the screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def menu():
    """Displays zendir menu/navigation"""
    print("""
##################################
Zendir
##################################
Welcome to zendir. Choose an option below.
1) Enter "scan" to scan/update file directory
2) Enter "hash" to build comparison hash for files
3) Enter "both" to perform both option 1 and 2
4) Enter "exit" to leave this program
""")
    choice = str(raw_input("Enter scan or hash to proceed: ")).lower()
    return choice

# top level directory to search (have to escape backslashes, f-u Windows)
def scan_directory(db):
	#for update portion
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

	print "\nSCAN/UPDATE COMPLETE!\n"
	print "{} files removed".format(deleteCount)
	print "{} files added".format(addCount)

def find_duplicates(db):
	cur = db.cursor()

	cur.execute("SELECT COUNT(*) FROM files WHERE sha1 IS NULL")
	hashesNeeded = cur.fetchone()[0]
	hashCount = 0
	cur.execute("SELECT * FROM files WHERE sha1 IS NULL")

	rows = cur.fetchall()

	for row in rows:
	    filepath = row[2] + row[3]
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

	    query = "UPDATE files SET sha1=%s, bytes=%s WHERE id=%s"
	    field_inserts = (sha1, bytes, row[0])

	    try:
	        cur.execute(query, field_inserts)
	        db.commit()
	    except MySQLdb.Error, e:
	        try:
	            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
	        except IndexError:
	            print "MySQL Error: %s" % str(e)

	    hashCount += 1
	    print "Hashes for {}: {} ({}/{})".format(row[3], sha1, hashCount, hashesNeeded)

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

	print "\nHASHING AND GROUPING DUPLICATES COMPLETE!\n"

ACTIVE = True
clear()
while ACTIVE is True:
	choice = menu()
	if choice == 'scan' or choice == 'both':
		scan_directory(db)
	if choice == 'hash' or choice == 'both':
		find_duplicates(db)
	if choice == 'exit':
		ACTIVE = False
