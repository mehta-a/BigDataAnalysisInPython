from neo4jrestclient.client import GraphDatabase
import csv
import os
from neo4jrestclient import client

no_of_weeks = 7
'''
Neo 4j graph params code below
'''

db = GraphDatabase("http://localhost:7474", username="neo4j", password="1234")
for x in range(0,no_of_weeks):
	u = []
	num = []
	i = 0
	csvStoragepath = '<Your Path to spark directory>'
	with open(csvStoragepath+'output/node4jinput/nodeLabel'+str(x)+'.csv', 'rb') as csvfile:
		nodereader = csv.reader(csvfile, delimiter=',')
		for row in nodereader:
			user = db.labels.create(str(row[1]))
			m = db.nodes.create(name=str(row[0]))
			user.add(m)
			u.append(m)
			num.append(str(row[0]))
			i = i + 1
	with open(csvStoragepath+'output/node4jinput/edgeWeight'+str(x)+'.csv', 'rb') as csvfile:
		relreader = csv.reader(csvfile, delimiter=',')
		for row in relreader:
			if str(row[0]) in num and str(row[1]) in num:
				u[num.index(str(row[0]))].relationships.create(str(row[2]),u[num.index(str(row[1]))])
	'''
	Query neo4j
	'''
	q = 'MATCH (n)-[r]->(m) RETURN n, r, m;'
	results = db.query(q, returns=(client.Node, str, client.Node))
	for r in results:
	    print("(%s)-[%s]->(%s)" % (r[0]["name"], r[1], r[2]["name"]))
	'''
	code to wait for user input to stop analyzing.	
	'''
	p = raw_input('Done Querying the graph? Let me know one you are done:[y?n]')
	if str(p)=='n':
		p = 'n'
	else:
		'''
		Clearing graph for new Graph
		'''
		q = 'START n = node(*) MATCH (n)-[r]-() WHERE (ID(n)>0 AND ID(n)<10000) DELETE n, r;'
		results = db.query(q, returns=(client.Node, str, client.Node))
		q = 'START n = node(*) MATCH ()-[r]-() DELETE r;'
		results = db.query(q, returns=(client.Node, str, client.Node))
		q = 'START n = node(*) MATCH n DELETE n;'
		results = db.query(q, returns=(client.Node, str, client.Node))
