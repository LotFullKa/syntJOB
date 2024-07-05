"""
In this file we want to define methods to
transform SQL text format to internal representation
"""

import re, os
from settings import RUNNER, DATABASE 
from utils import replaceAliasesInJoinConds, getJoinTblName, getColumns

def SQLQueryToAliases(query):
    """
    Generates table-alias dict from SQL query
    """
    aliases = query.split("FROM")[1]
    aliases = aliases.split("WHERE")[0]

    # in case we didn't have where clause
    aliases = aliases.split(";")[0] 
    aliases = aliases.strip()

    # mark aliases
    # XXX there is a better way to do this
    aliases = aliases.replace(" AS ", "->")
    aliases = aliases.replace(" as ", "->")

    # split aliases from each other
    aliases = aliases.replace(",", " ")
    aliases = aliases.replace("\n", " ")
    aliases = aliases.replace("\t", " ")
    aliases = aliases.split(" ")

    # remove empty lines
    aliases = list(filter(lambda el: True if el else False, aliases))
    for i, al in enumerate(aliases):
        pair = al.split("->")[::-1]
        if len(pair) == 1:
            pair = [pair[0] , pair[0]]
        aliases[i] = pair
    aliases = dict(aliases)
    return(aliases)

def SQLQueryToJoinConds(query):
    """
    Generates list of join conditions from SQL query
    """
    aliases = SQLQueryToAliases(query)
    # joinTblReg is a regexp that stands for table names or aliasses
    joinTblRegExp = "("
    joinTblRegExp += "|".join(aliases.keys()) 
    joinTblRegExp += "|"
    joinTblRegExp += "|".join(aliases.values())
    joinTblRegExp += ")"
    # joinCondReg is a regexp that stands for join condition
    joinCondReg = fr"({joinTblRegExp}\.\w+ = {joinTblRegExp}\.\w+)"
    
    joinConds = re.findall(joinCondReg, query)
    resConds = []
    oldJoinConds = []
    for joinCond in joinConds:
        oldJoinCond = joinCond[0]
        joinCond = replaceAliasesInJoinConds(joinCond[0], aliases) 
        resConds.append(joinCond)
        oldJoinConds.append(oldJoinCond)

    return resConds, oldJoinConds


def getTableDDL(joinCond):
    table_name = getJoinTblName(joinCond)
    joinTbls = re.search(r'(\w+).\w+ = (\w+).\w+', joinCond)
    joinTbls = [joinTbls.group(1), joinTbls.group(2)]
    columns = getColumns(joinCond)

    SQLTemplate = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name}
    AS 
        SELECT DISTINCT {columns} 
        FROM {joinTbls[0]},
            {joinTbls[1]} 
        WHERE {joinCond};
    """

    return SQLTemplate
