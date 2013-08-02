#!/usr/bin/env python

import ndn
import sys

import json
import pymongo
from pymongo import Connection

connection = Connection()
dtb = connection.ndnfs
src = dtb.root

face = ndn.Face()

InterestBaseName = ndn.Name ("/ndn/ucla.edu/irl/caelean/mongo-query/simple-fetch")
COUNTER = 0

def OnInterest (name, interest):
    global COUNTER
    print >> sys.stderr, "<< PyNDN %s" % interest.name
    
    queryname = str(ndn.Name (interest.name[len(InterestBaseName):-1]))
    print queryname
    
    isFolder = {}
    constituents = []  

#     content = "PyNDN LINE #%d\n" % COUNTER
    
    obj = src.find_one({"_id":queryname, "type":0})
    if obj != None:
        print obj['data']
        for i in obj['data']:
            if queryname == '/':
                constituents.append(src.find_one({"_id":queryname + i}))
            else:
                constituents.append(src.find_one({"_id":queryname + '/' + i}))          
        print obj['data']
        obj['constituents'] =constituents
        
    content = json.dumps(obj)
    
    print content
#     COUNTER = COUNTER + 1
    
    data = ndn.ContentObject(name = interest.name, content = content, 
                            signed_info = ndn.SignedInfo(key_digest = ndn.Key.getDefaultKey ().publicKeyID, 
                            freshness = 5))
    data.sign (ndn.Key.getDefaultKey ())
    
    face.put (data)

    return ndn.RESULT_OK

face.setInterestFilterSimple (InterestBaseName, OnInterest)

while True:
    face.run (500)