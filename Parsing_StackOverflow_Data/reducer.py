'''
        This is the Reducer script to join the posts, Post-answers,
        post-comments and answer-comments recieved from mapper.
        1. This initially parses the Value data recieved from mapper.
        2. Builds inverse dictionary of <Key:answer, Value:Post-IDs>
        3. Builds inverse dictionary of <Key:comments, Value:Parent-post-or-ans-Ids>
        4. Finally reads individual inverse-dictionaries and creates a final dictionary.
        5. Finally data is printed to standard output in json format.
'''
#!/usr/bin/env python
import sys
sys.path.append('.')
import json
from pprint import pprint
import collections
from collections import OrderedDict
import ast


def search(key,dict1):
        if key in dict1:
                return inverse_comments[str(key)]
        return []
for line in sys.stdin:
        line = line.split('@')
        final =OrderedDict()
        final['id']=int(line[0])
        final['Ques_id']=[]

        posts = ast.literal_eval(line[1])
        answers = ast.literal_eval(line[2])
        comments = ast.literal_eval(line[3])
        
        inverse_answer = defaultdict(list)
        for j in answers:
                for i in answers[i]:
                        inverse_answer[i].append(j)

        inverse_comments = defaultdict(list)
        for j in comments:
                for i in comments[i]:
                        inverse_comments[i].append(j)        
        #print posts
        #print "These were posts"
        for x in range(0,len(posts)):
                res = OrderedDict()
                res['id']=posts[x]
                res['comments']=[]
                temp_comment=[]
                if str(posts[x]) in inverse_comments.keys():
                        temp_comment = inverse_comments[str(posts[x])]
                res['comments']=temp_comment
                res['answers']=[]
                #print temp_comment
                if str(posts[x]) in inverse_answer.keys(): 
                        #print "test",str(posts[x])
                        temp_answer=OrderedDict()
                        if len(inverse_answer[str(posts[x])])>1:
                                for ansId in inverse_answer[str(posts[x])]:
                                        temp_answer['id']=ansId
                                        temp_answer['comments']=search(ansId,inverse_comments)
                                res['answers'].append(temp_answer)
                        
                        else:
                                temp_answer['id']=inverse_answer[str(posts[x])]
                                posts[x]=(inverse_answer[str(posts[x])])[0]
                                temp_answer['comments']=search(posts[x],inverse_comments)
                                res['answers'].append(temp_answer)
                final['Ques_id'].append(res)
                print str(json.dumps(final))