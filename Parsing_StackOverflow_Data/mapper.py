'''
    This is the mapper script to read path of data directory
    from standard input(Pass it as an -input option while running 
    the command). 
    1. It takes the Posts.xml and Comments.xml
    2. Builds list of postsIds
    3. Builds dictionary of Post-answers-Ids and their respective post Ids.
    4. Builds dictionary of Post or Answer Comment-Ids and their respective parent Ids.
    5. It passes to Reducer this data with a key 1(This can be modified if you parse multiple Posts and comments files), and value as other relevant data structures.
'''
#!/usr/bin/env python
import sys
sys.path.append('.')
import os
import xml.etree.ElementTree as ET
import json

for i in sys.stdin:
    postList = []
    answerDict = {}
    commentDict = {}
    i = i.strip()

    tree = ET.parse(str(i)+"/"+"Posts.xml")
    root = tree.getroot()
    for row in root.iter('row'):
        if(row.attrib['PostTypeId']=='1'):
            postList.append(row.attrib['Id'])
        if(row.attrib['PostTypeId']=='2'):
            answerDict[row.attrib['Id']] = row.attrib['ParentId']

    tree = ET.parse(str(i)+"/"+"Comments.xml")
    root = tree.getroot()
    for row in root.iter('row'):
        commentDict[row.attrib['Id']] = row.attrib['PostId']
        
    print '%d@%s@%s@%s' % (1, postList, str(json.dumps(answerDict)), str(json.dumps(commentDict)))