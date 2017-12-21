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
#    cur.execute("""
#    CREATE TABLE IF NOT EXISTS removedDuplicates
#    (
#      id int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
#      removedFile varchar(255) binary NOT NULL,
#      removedFrom varchar(255) binary NOT NULL,
#      remiainingLocation varchar(255) binary NOT NULL
#    );
#    """
 #   )


#Pull all distince shas from scanned drive and create a list of them
cur.execute("""
SELECT DISTINCT remiainingLocation FROM removedDuplicates
""")

finalLocations = cur.fetchall()
locationlist = []
for location in finalLocations:
    locationlist += location

countLocation = len(locationlist)
destinationID = 0
originID = countLocation

for location in locationlist:
    t = (location, )
    destinationID += 1
    destinationFolder = location

    cur.execute('SELECT * FROM removedDuplicates WHERE remiainingLocation=%s', t)
    results = cur.fetchall()

    for result in results:
        originLocation = result[2]
        originName = result[1]
        originID += 1

        with open("graph.txt", "ab") as myfile:
            myfile.write("{}[\"{}\"] -->|\"{}\"|{}[\"{}\"]\n".format(originID, originLocation, originName, destinationID, destinationFolder))

print('graph generated')
