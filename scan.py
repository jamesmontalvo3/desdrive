#!/usr/bin/env python

import os, time, MySQLdb, hashlib
from os.path import join, getsize
import config

db = MySQLdb.connect(host=config.database["host"],
					user=config.database["user"],
					passwd=config.database["passwd"],
					db=config.database["db"])
cur = db.cursor()

# top level directory to search (have to escape backslashes, f-u Windows)
rootpath = config.directoryToAnalyze
print "Scanning files at ", rootpath

for dirpath, dirs, files in os.walk(rootpath):
	for filename in files:

		filepath = join(dirpath, filename)

		print "Scanning %s" % filepath

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
	# 	dir = MySQLdb.escape_string(dir)
	print dir
	print "INSERTING dir: {0}".format(dir)
	try:
		cur.execute( "INSERT INTO directories (path) VALUES ( %(dir)s )", { 'dir': dir } )
		db.commit()
	except MySQLdb.Error, e:
		try:
			print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
		except IndexError:
			print "MySQL Error: %s" % str(e)

# Close communication with the database
cur.close()

print "complete generating directory listings"
