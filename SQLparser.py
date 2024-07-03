"""
In this file we want to define methods to
transform SQL text format to internal representation
"""

import re, os
from settings import RUNNER, DATABASE 

def execSQL(SQLcmd):
    stream = os.popen(f"{RUNNER} {DATABASE} -c \"{SQLcmd}\"")
    result = stream.read()
    headings = result.split("\n")[0].split()
    content = "\n".join(result.split("\n")[1:])
    return result

def getTableOid(tableName):
    SQLTemplate = f"""
    SELECT relname, oid FROM pg_class
    WHERE relname='{tableName}';
    """
    rawRes = execSQL(SQLTemplate)
    oid = re.search(rf'{tableName}\s+\|\s+(\d+)', rawRes).group(1)
      
    return oid

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
    joinConds = [i[0] for i in joinConds]

    return(joinConds)

def replaceAliasesInJoinConds(join,aliases):
    """
    modify join condition and replace aliases with tableNames
    """
    joinTbls = join.split()
    joinTbls = sorted([joinTbls[0], joinTbls[-1]])

    joinFields = ", ".join(joinTbls)
    joinAliases = [i.split(".")[0] for i in joinTbls]
    joinTbls = [aliases[i.split(".")[0]] for i in joinTbls]

    columns = joinFields.split(",")
    columns = [i.strip() for i in columns]
    columns = [i+" AS "+i.replace(".","_") for i in columns]
    columns = ", ".join(columns)

    return(joinTbls, joinAliases, columns)

def getOidedTableName(tblName):
    print(tblName)
    return 't' + getTableOid(tblName.split(".")[0])+"."+tblName.split(".")[-1]

def getTableDDL(joinCond):
    columns = sorted(joinCond.split(" = "))
    tbl_names = [getOidedTableName(col) for col in columns]
    table_name = "_EQ_".join(tbl_names)
    table_name = table_name.replace(".","__")
    joinTbls = [i.split(".")[0] for i in columns]
    columns = [i.strip() for i in columns]
    aliases = [getOidedTableName(col).replace(".","_") for col in columns]
    columns = [i + " AS " + alias for i, alias in zip(columns, aliases)]
    columns = ", ".join(columns)

    
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
