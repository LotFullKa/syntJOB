"""
In this file we implement logick of working with workloads
"""

import os
from settings import WORKLOAD_DIR
from SQLparser import SQLQueryToAliases, SQLQueryToJoinConds 

"""
Generator that iterates through queries in workload
""" 
def queries(workload=WORKLOAD_DIR):
    size = len(os.listdir(workload))
    listdir = filter(lambda file: file.endswith(".sql"), os.listdir(WORKLOAD_DIR))
    for i, file in enumerate(listdir):
        with open(WORKLOAD_DIR + '/' + file, "r") as f:
            query = f.read()
            percent = i/size
            percent = int(percent * 100)
            #print("loadbar: " + '[' + '#'*percent + '-'+(100-percent) + ']')
            yield(file, query)

def locJoinCondToGlob(joinCond):
    return null
"""
Iterate over queries in workload and 
return a set of join conditions

Note, table aliases in join conditions
are replaced with original table names
"""
def  getGlobJoinConds(workload=WORKLOAD_DIR):
    globalJoinConds = set()
    for file, query in queries(workload):
        aliases = SQLQueryToAliases(query)
        joinConds = SQLQueryToJoinConds(query)
        for joinCond in joinConds:
            condition = sorted(joinCond.split(" = "))
            condition = [aliases[i.split(".")[0]]+"."+i.split(".")[-1] for i in condition]
            joinCond = " = ".join(condition)
            globalJoinConds.add(joinCond)
    return(globalJoinConds)

def getOIDJoinConds(workload=WORKLOAD_DIR):
    pass
