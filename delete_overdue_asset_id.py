# -*- coding: utf-8 -*-
"""
@author: qingya@cisco.com
"""

import datetime
import sqlite3

list_id = list()
list_asset_id = list()
list_goid = list()
list_ingest_time = list()
list_ingest_time2 = list()

file_input = open('all-content.small')
file_asset_id = open('all-content-Asset-id', 'r')
file_goid = open('all-content-GOID', 'r')
file_time = open('all-content-time', 'r')
#can be optimized to read all-content file

# Create DB file.

conn = sqlite3.connect('asset_db.sqlite3')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS asset;
DROP TABLE IF EXISTS asset_2;

CREATE TABLE asset(
    id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    asset_id    TEXT UNIQUE,
    ingest_time    NUMBERIC,
    ingest_time2   NUMBERIC,
    goid    NUMBERIC
)
''')

for line in file_asset_id:
    asset_id = line.split()[1][1:]
    list_asset_id.append(asset_id)

for line in file_goid:
    goid = line.split()[2][1:]
    list_goid.append(goid)

for line in file_time:
    ingest_time = line.split()[2][1:]
    list_ingest_time.append(ingest_time)
    ingest_time2 = datetime.datetime.fromtimestamp(int(ingest_time))
    list_ingest_time2.append(ingest_time2)

for i in range(len(list_asset_id)):
    cur.execute('INSERT INTO asset (asset_id, goid, ingest_time, ingest_time2) VALUES ( ?, ?, ?, ?)', (list_asset_id[i], list_goid[i], list_ingest_time[i], list_ingest_time2[i]))

conn.commit()

#output #B# file before 2016.2.15

cur.execute("SELECT asset_id, ingest_time2 FROM asset WHERE (ingest_time < 1455465600  AND asset_id LIKE '%#B#')")

count = 0

'''
file_output.write('AssetID\t\t\t\tIngest time'+'\n\n')

for row in cur:
    file_output.write(str(row) + '\n')
    count += 1

file_output.write('count=' + str(count) + '\n')
'''

#output time-shift asset id like 1::..... before 14 days
# select * from asset where ingest_time < 1455206400 and asset_id like '1::%'
#
# creat a new table asset_2 to store select result
# create table asset_2 as select * from asset where ingest_time < 1455206400 and asset_id like '1::%'
#cur.execute("SELECT asset_id, ingest_time2 FROM asset WHERE (ingest_time < 1455206400  AND asset_id LIKE '1::%')")

cur.execute("CREATE TABLE asset_2 AS SELECT * FROM asset WHERE ingest_time < 1455206400 AND asset_id like '1::%'")

count = 0

file_output = file('overdue_asset_id.txt', 'w')
file_output2 = file('overdue_asset_id-2.txt', 'w')

file_output.write('AssetID\t\t\t\tIngest time'+'\n\n')

file_input2 = file('cisco.txt', 'r')
# topway source file

for line in file_input2:
    cur.execute("SELECT asset_id, ingest_time2 FROM asset_2 WHERE asset_id == ( ? )", (line.strip(), ) )
    file_output2.write(str(cur.fetchone()) + '\n')
    cur.execute("DELETE FROM asset_2 WHERE asset_id == (?)", (line.strip(),))

conn.commit()
cur.close()
file_output.close()
file_output2.close()

'''
for line in file_input:
    if line.startswith('Name'):
        list_id.append(id) 
        list_asset_id.append(re.findall(':(.+)', line)[0])
    if line.startswith('1X GOID'):
        list_goid.append(re.findall(':(\d+)', line)[0])
    if line.startswith('Creation Time') :
        ingest_time = re.findall(':(\d+)', line)[0]
        list_ingest_time.append(datetime.datetime.fromtimestamp(int(ingest_time)))

#        if ingest_time > :

#1455465600, 2016.2.15, 0:00:00
'''



