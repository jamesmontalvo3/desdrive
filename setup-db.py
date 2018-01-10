#!/usr/bin/env python
#
# Setup database. Re-running this will wipe out your database!

import MySQLdb
import config

db = MySQLdb.connect(host=config.database["host"],
					user=config.database["user"],
					passwd=config.database["passwd"])

cur = db.cursor()

# Drop database, create database, use database
print "Drop database, recreate, use..."
cur.execute("DROP DATABASE IF EXISTS %(db)s" % config.database)
cur.execute("CREATE DATABASE %(db)s" % config.database)
cur.execute("USE %(db)s" % config.database)


# PostgreSQL had:
# CREATE TABLE IF NOT EXISTS files (
# id serial PRIMARY KEY,
# filename varchar(255),
# extension varchar(255),
# bytes bigint,
# root text,
# relativepath text,
# sha1 varchar(40),
# created varchar(19),
# modified varchar(19),
# accessed varchar(19)
# );

# Other things to add maybe...
# --
# -- add even later
# --   parent directory bytes
# --   parent directory files
# --   grandparent dir bytes
# --   grandparent dir files
# --
# -- probably never
# --   parent dir like-file bytes
# --   parent dir like-file files
# --


# create table files
print "Create `files` table..."
cur.execute("""
CREATE TABLE files (
  id int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
  fullpath varchar(255) binary NOT NULL,
  rootpath varchar(255) binary NOT NULL,
  relativepath varchar(255) binary NOT NULL,
  filename varchar(255) binary NOT NULL,
  extension varchar(10) binary NOT NULL default '',
  bytes bigint NOT NULL default 0,
  sha1 varbinary(40),
  blockhash varbinary(64),
  created varchar(19),
  modified varchar(19),
  accessed varchar(19),
  owner varchar(19),
  is_dupe tinyint
);

CREATE UNIQUE INDEX path ON files (rootpath, relativepath);
CREATE INDEX relativepath ON files (relativepath);
CREATE INDEX extension ON files (extension);
CREATE INDEX sha1 ON files (sha1);
CREATE INDEX blockhash ON files (blockhash);
CREATE INDEX is_dupe ON files (is_dupe);
""")


#
# Below previously in buildDirs.py
#
import os, time
from os.path import join, getsize


print "Create `directories` table..."
cur = db.cursor()

cur.execute("""
CREATE TABLE directories (
  id int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
  path varchar(255) binary NOT NULL,
  num_files INT UNSIGNED,
  num_dupes INT UNSIGNED,
  total_bytes BIGINT UNSIGNED,
  dupe_bytes BIGINT UNSIGNED,
  last_scan varbinary(14)
);

CREATE UNIQUE INDEX path ON directories (path);
""")

# Close communication with the database
cur.close()


print "Database setup complete"
