from pyspark import SparkConf, SparkContext
import collections
import csv
from collections import namedtuple
from pyspark.sql import Row
from pyspark.sql import SQLContext
import os
from datetime import datetime,timedelta
import datetime
import webbrowser

from neo4jrestclient.client import GraphDatabase

registered_num = []

#ID0, CALLING_NUM1, CALLED NUMBER2, START TIME3, END TIME4, CALL TYPE5, CHARGE6, CALL RESULT7
fields = ('userID', 'calling_num', 'called_num', 'startTime', 'endTime', 'callType','callResult','churn')
User = namedtuple('User', fields)

def preprocess(x):
	x = x.split(',')
	for i in range(len(x)):
		x[i] = str(x[i])
	#adding to userid
	if x[1] not in registered_num:
		registered_num.append(x[1])
	if x[2] not in registered_num:
		registered_num.append(x[2])
	#print len(registered_num)
	return

#Get graph  relation csv
def getGraphRelation(x):
	y = ()
	if(type(x)!=type(y)):
		x = x.split(',')
		for i in range(len(x)):
			x[i] = str(x[i])
	graph_startnode = x[2]	#calling NUM   	This will also be its key
	graph_endnode = x[3] #called num
	key = str(graph_startnode)+'|'+str(graph_endnode)
	edgeWeight = x[1]
	return key, edgeWeight

#get Pulse
def getPulse(startTime,endTime):
	t = startTime.split('T')[1].split(':') #2016-05-06T14:40:15.213+05:30
	hr = int(t[0])
	mn = int(t[1])
	sec = float(t[2].split('+')[0])
	t2 = endTime.split('T')[1].split(':')
	hr2 = int(t2[0])
	mn2 = int(t2[1])
	sec2 = float(t2[2].split('+')[0])
	hrdiff = hr2 - hr
	mndiff = mn2 - mn
	secdiff = sec2 - sec
	pul = (((hrdiff*60*60 + mndiff*60 + secdiff)%60)+1)
	d = startTime.split('T')[0]
	return d,pul

#divide into date key
def arrangeDateWiseFrames(x):
	x = x.split(',')
	for i in range(len(x)):
		x[i] = str(x[i])
	dat,pulse = getPulse(x[3],x[4])
	return dat,pulse,x[1],x[2],x[5],x[8]#date, pulse, calling, called, calltype, churn

#parsefile
def getUser(x):
	x = x.split(',')
	for i in range(len(x)):
		x[i] = str(x[i])
	user_row = User(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7])
	return user_row

def getDate(dRange):
	datelist = dRange
	least = datelist[0]
	most = datelist[len(datelist)-1]
	least = least.split('-')
	l = ''
	l = l + least[2] + least[1] + least[0]
	most = most.split('-')
	m = ''
	m = m + most[2] + most[1] + most[0]
	l = datetime.datetime.strptime(l, "%d%m%Y")
	m = datetime.datetime.strptime(m, "%d%m%Y") 
	return l, m

def getWeeks(d1, d2):
	monday1 = (d1 - timedelta(days=d1.weekday()))
	monday2 = (d2 - timedelta(days=d2.weekday()))
	return (monday2 - monday1).days / 7

def addSevenDays(oldDate):
	plusSeven = oldDate + datetime.timedelta(days=7)
	return plusSeven

def dateToString(d):
	s = d.strftime('%Y-%m-%d')
	return s

def stringToDate(s):
	s = s.split('-')
	l = ''
	l = l + s[2] + s[1] + s[0]
	d = datetime.datetime.strptime(l, "%d%m%Y")
	return d

def writeEdgecsv(edgeRdd, i):
	fedge = open('output/node4jinput/edgeWeight'+str(i)+'.csv', 'a')
	edgeList = edgeRdd.collect()
	for x in edgeList:
		weight = x[0]
		node = x[1].split('|')
		fedge.write(str(node[0])+','+str(node[1])+','+str(weight)+'\n')
	fedge.close()
	return

def findChurnInThisWeek(weekRdd, i):
	weekList = weekRdd.collect()
	nodeDict = {}
	for m in weekList:
		x = m[2]
		l = m[5]
		nodeDict[x] = l
	fweek = open('output/node4jinput/nodeLabel'+str(i)+'.csv', 'a')
	for x in nodeDict.keys():
		if(nodeDict[x] == 'false'):
			label = 'non-churner'
		else:
			label = 'churner'
		fweek.write(str(x)+','+str(label)+'\n')
	fweek.close()
	return
	
def graphParam():
	return

def main(sc):
	lines = sc.textFile("/Users/ankita_mehta/Desktop/BDA_Project/cdr_Dataset2.csv")
	#line_rdd = lines.map(getUser)
	parts = lines.map(lambda l: l.split(","))
	#Unique list of users... 1. userid 2. userno
	user = parts.map(lambda p: (p[0], p[1].strip())).distinct()
	#Divide them in time frames
	dateKeyRdd = lines.map(arrangeDateWiseFrames)
	dateKeyRdd = dateKeyRdd.sortByKey()
	#dateKeyRdd.coalesce(1).saveAsTextFile("output/dateKeyRdd6")

	#get unique date ranges
	dateRange = dateKeyRdd.keys().distinct()
	l, m = getDate(sorted(dateRange.collect())) #returns min and max date range
	no_of_weeks = getWeeks(l,m)	#gets no of weeks of data we have
	
	
	#Now we split of data into weekly based Rdds
	weeksRdd = []
	edgeWeightRdd = []
	
	for i in range(0,no_of_weeks):
		weeksR = dateKeyRdd.filter(lambda (k,v1,v2,v3,v4,v5): stringToDate(k).date()>=l.date() and stringToDate(k).date()<=addSevenDays(l).date())
		weeksRdd.append(weeksR)
		edgeWeight = weeksR.map(getGraphRelation).reduceByKey(lambda x, y: x + y)
		edgeWeight = edgeWeight.map(lambda (x,y): (y,x)).sortByKey()
		edgeWeightRdd.append(edgeWeight)
		l = addSevenDays(l)

	#We find the relation between two nodes and its edge weight
	#Write csv for node4j format
	i = 0
	for i in range(0,no_of_weeks):
		#write Edge
		filename = 'output/node4jinput/edgeWeight'+str(i)+'.csv'
		try:
			os.makedirs(os.path.dirname(filename))
		except:
			p = 0
		fedge = open(filename, 'w')
		fedge.write(':START_ID,:END_ID,:TYPE\n')
		fedge.close()
		writeEdgecsv(edgeWeightRdd[i], i)
		#Write Node
		filename = 'output/node4jinput/nodeLabel'+str(i)+'.csv'
		try:
			os.makedirs(os.path.dirname(filename))
		except:
			p = 0	
		fweek = open(filename, 'w')
		fweek.write('personId:ID,:LABEL\n')
		fweek.close()
		findChurnInThisWeek(weeksRdd[i], i)
	#lets see if output comes here
	#weeksRdd[0].coalesce(1).saveAsTextFile("output/weekKeyRdd-4")
	#edgeWeightRdd[0].coalesce(1).saveAsTextFile("output/edgeWeightRdd-4")

	#We find the relation between two nodes and its edge weight
	edgeWeight = lines.map(getGraphRelation).reduceByKey(lambda x, y: x + y)
	edgeWeight = edgeWeight.map(lambda (x,y): (y,x)).sortByKey()
	#edgeWeight.coalesce(1).saveAsTextFile("output/edgeWeightRdd6")
	return

if __name__ == "__main__":
	conf = SparkConf().setMaster("local").setAppName("ChurnPrediction")
	sc = SparkContext(conf = conf)
	main(sc)
