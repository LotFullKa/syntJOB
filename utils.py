import re, os
from settings import RUNNER, DATABASE 

def execSQL(SQLcmd):
    stream = os.popen(f"{RUNNER} {DATABASE} -c \"{SQLcmd}\"")
    result = stream.read()
    headings = result.split("\n")[0].split()
    content = "\n".join(result.split("\n")[1:])
    return result

def getTableOid(tableName):
    tableName = tableName.lower()
    SQLTemplate = f"""
    SELECT relname, oid FROM pg_class
    WHERE relname='{tableName}';
    """
    rawRes = execSQL(SQLTemplate)
    oid = re.search(rf'{tableName}\s+\|\s+(\d+)', rawRes).group(1)
      
    return oid

def replaceAliasesInJoinConds(join, aliases):
    """
    modify join condition and replace aliases with tableNames
    """
    joinTbls = join.split(" = ")    
    resTbls = []
    for tbl in joinTbls:
        onlytbl = tbl.split(".")[0]
        resTbls.append(tbl.replace(onlytbl, aliases[onlytbl]))

    return " = ".join(resTbls)

def getOidedTableName(tblName):
    return 't' + getTableOid(tblName.split(".")[0]) + "_" + tblName.split(".")[-1]


def getJoinTblName(joinCond):
    conditions = joinCond.split(' = ')
    return getOidedTableName(conditions[0]) + "__EQ__" + getOidedTableName(conditions[1]) 

def getColumns(joinCond):
    realColumns = joinCond.split(" = ")  
    aliases = getFields(joinCond)

    return f"{realColumns[0]} AS {aliases[0]}, {realColumns[1]} AS {aliases[1]}"

def getFields(joinCond):
    aliases = joinCond.split(" = ")
    # aliases = ['f' + str(hash(al.replace(".", "_")))[1:8] for al in aliases]
    aliases = ['left_field', 'right_field']

    return aliases

def replaceGlobalNameInJoinConds(joinCond, aliases):
    realiases = {value: key for key, value in aliases.items()}
    tblNames = joinCond.split(" = ")
    result = []
    for name in tblNames:
        name, field = name.split('.')
        rename = realiases[name]
        res = rename + "." + field
        result.append(res)
    return " = ".join(result)
