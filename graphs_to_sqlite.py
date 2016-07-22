import sys
import os
import json
import networkx as nx
import sqlite3

conn = sqlite3.connect(sys.argv[1])
curs = conn.cursor()

curs.execute('CREATE TABLE IF NOT EXISTS edges ("src" TEXT, "tar" TEXT, "data" TEXT, "date" TEXT);')
file1 = open(sys.argv[2], 'r')
for line in file1:
	line = line.strip()
	print line
	G = nx.read_graphml(line)
	date = os.path.splitext(os.path.basename(line))[0]
	for src, tar, data in G.edges_iter(data=True):
		curs.execute('INSERT OR IGNORE INTO edges VALUES (?, ?, ?, ?)', (src, tar, json.dumps(data), date))

conn.commit()

curs.close()
conn.close()

