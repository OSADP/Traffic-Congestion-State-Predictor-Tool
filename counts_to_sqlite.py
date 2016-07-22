import sys
import sqlite3
import csv
import datetime
import time

conn = sqlite3.connect(sys.argv[1])
curs = conn.cursor()


with open(sys.argv[2], 'r') as file1:
    
    for line in file1:
        path = line.strip()
        database = path.split('_')[2]
        print database

        curs.execute("""
            CREATE TABLE IF NOT EXISTS '%s' (
                'id' INTEGER,
                'dt' INTEGER,
                'dir' INTEGER,
                'speed' REAL,
                'bi' INTEGER
            );
        """ % (database))
        curs.execute("""
            CREATE INDEX IF NOT EXISTS 'idx_dt' ON '%s' (
                'dt' ASC
            );
        """ % (database))
    
        with open(path, 'r') as fh:
            SQL = "INSERT INTO '%s' VALUES (?, ?, ?, ?, ?);" % (database)
            reader = csv.reader(fh)
            for row in reader:
                dt = datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")
                row[1] = time.mktime(dt.timetuple())
                curs.execute(SQL, row)

conn.commit()

curs.close()
conn.close()




