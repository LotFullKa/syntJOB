import os
import re

WORKLOAD_DIR = "./"

# Generator that iterates through all workload
def queries():
    size = len(os.listdir(WORKLOAD_DIR))
    listdir = list(filter(lambda file: file.endswith(".sql"), os.listdir(WORKLOAD_DIR)))
    for i, file in enumerate(listdir):
        with open(WORKLOAD_DIR + '/' + file, "r") as f:
            query = f.read()
            percent = i/size
            percent = int(percent * 100)
            print("loadbar: " + '[' + '#'*percent + '-'*(100-percent) + ']')
            yield(file, query)

# parse sql query, get:
# tables aliases and join conditions
def parseQuery(query):
    aliases = query.split("FROM")[1]
    aliases = aliases.split("WHERE")[0]

    # in case we didn't have where clause
    # aliases = aliases.split(";")[0] 

    aliases = re.findall(r"[^\s,]+ AS [^\s,]+", aliases)
    aliases = list(aliases)
    aliases = [i.strip().replace(",","") for i in aliases]
    aliases = [i.split(" AS ")[::-1] for i in aliases]
    aliases = dict(aliases)

    joinConds = re.findall(r"\w+\.\w+ = \w+\.\w+", query)
    joinConds = list(joinConds)

    return(aliases, joinConds)

def parseJoinCond(join,aliases):
    joinTbls = join.split()
    joinTbls = [joinTbls[0], joinTbls[-1]]

    joinFields = ", ".join(joinTbls)
    joinAliases = [i.split(".")[0] for i in joinTbls]
    joinTbls = [aliases[i.split(".")[0]] for i in joinTbls]

    columns = joinFields.split(",")
    columns = [i.strip() for i in columns]
    columns = [i+" AS "+i.replace(".","_") for i in columns]
    columns = ",".join(columns)

    return(joinTbls, joinAliases, columns)

def getSQL(fileName,joinAliases,joinTbls,columns,join):
    columns = columns.split(",")
    distinctOn = columns[0].split("AS")[0]+","+columns[1].split("AS")[0]
    distinctAliases = columns[0].split("AS")[1]+","+columns[1].split("AS")[1]
    SQLTemplate =f"""
    DROP TABLE if EXISTS f{fileName}_{joinAliases[0]}_{joinAliases[1]};
    CREATE TABLE f{fileName}_{joinAliases[0]}_{joinAliases[1]}
    as 
        select distinct {columns[0]}, {columns[1]} 
        from {joinTbls[0]} as {joinAliases[0]},
            {joinTbls[1]} as {joinAliases[1]} 
        where {join}
    """
    return SQLTemplate

def createTables():
    for file, query in queries():
        fileName = file.split(".")[0]
        aliases,joinConds = parseQuery(query)

        for join in joinConds:
            joinTbls, joinAliases, columns = parseJoinCond(join,aliases)
            #SQLTemplate = SQLTemplate.replace("\n","")
            table = getSQL(fileName,joinAliases,joinTbls,columns,join)
            print(table)
            stream = os.popen(f"{RUNNER} {DATABASE} -c \"{table}\"")
            stream.read()

def updateWorkload():
    for file, query in queries():
        fileName = file.split(".")[0]
        aliases,joinConds = parseQuery(query)

        for join in joinConds:
            joinTbls, joinAliases, columns = parseJoinCond(join,aliases)
            joinFields = ", ".join(joinTbls)
            joinFields = columns.split(",")
            joinFields = [i.split("AS")[0] for i in joinFields]
            joinFields = [i.replace(".","_").strip() for i in joinFields] 

            newJoinCond = join.split(" = ")
            newJoinCond[0] += f" = f{fileName}_{joinAliases[0]}_{joinAliases[1]}"
            newJoinCond[0] += f".{joinFields[0]}"
            newJoinCond[1] += f" = f{fileName}_{joinAliases[0]}_{joinAliases[1]}"
            newJoinCond[1] += f".{joinFields[1]}"
            newJoinCond = "\n  AND ".join(newJoinCond)
            newJoinCond = newJoinCond.replace(",","")

            query = query.replace(join,newJoinCond)
            query = query.replace("FROM",f"FROM\n   f{fileName}_{joinAliases[0]}_{joinAliases[1]},")

        ext = ".sql"
        fileName += "_mod." + ext
        print(file) # this is needed for sence of progress

        with open(f"mod/{file}","w") as f:
            f.write(query)

if __name__=="__main__":
    DATABASE = "synt"
    RUNNER = 'gsql'

    createTables()
    updateWorkload()