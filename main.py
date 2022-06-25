# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import json
import sys
import cx_Oracle
import pymysql

def GetMysqlColumn(SourceJdbc,TableName,SourceUser,SourcePassword):
    JdbcSplit=SourceJdbc.split(':')
    Ip=JdbcSplit[2].replace('//','')
    PortDb=JdbcSplit[3].split('/')
    Column=[]
    conn=pymysql.connect(host=Ip,user=SourceUser,password=SourcePassword,database=PortDb[1],charset='utf8',port=int(PortDb[0]))
    cursor=conn.cursor()
    sql="desc " + TableName
    cursor.execute(sql)
    result=cursor.fetchall()
    for row in result:
        Column.append(row[0])
    print(Column)
    return Column

def GetOracleColumn(SourceJdbc, TableName, SourceUser, SourcePassword):
    TableName = TableName.split('.')
    JdbcSplit=SourceJdbc.split(':')
    Ip=JdbcSplit[3].replace('@','')
    dns_tns=cx_Oracle.makedsn(Ip,JdbcSplit[4],sid=JdbcSplit[5])
    conn = cx_Oracle.connect(SourceUser,SourcePassword,dns_tns)
    cur = conn.cursor()
    Column=[]
    PkColumn=""
    sql = "select column_name from all_tab_columns where table_name =\'" + TableName[1] + "\' and owner=\'" + TableName[
        0] + "\'"
    cur.execute(sql)
    result = cur.fetchall()
    for row in result:
        Column.append((row[0]))
    sql = "select cu.column_name from all_cons_columns cu,all_constraints au where cu.CONSTRAINT_NAME= au.CONSTRAINT_NAME and cu.owner=au.owner and au.constraint_type = 'P' and au.TABLE_NAME=\'"+ TableName[1] + "\'and cu.owner =\'" + TableName[0] +"\'"
    cur.execute(sql)
    PkResult=cur.fetchall()
    for row in PkResult:
        PkColumn=PkColumn+row[0]+"，"
    cur.close()
    conn.close()

    return Column,PkColumn

def MakeJson(SourceUser, SourcePassword, SourceName, DestinationUser, DestinationPassword, DestinationName,
             ErrorRecode, SourceTableName,  SourceJdbc, DestinationJDBC,Column,DestinationTableName,PkColumn,WriteMode):
    dict = {
"core":
  {"transport":
    {"channel":
      {"speed":
        {"byte":"-1","record":"-1"}
      }
    }
  },
  "job": {
        "content": [
            {
                "reader": {
                    "name": SourceName,
                    "parameter": {
                        "username":SourceUser,
                        "password":SourcePassword,
                        "column" : Column,
                     "splitPk":PkColumn,
                     "connection":[
                            {
                                  "table":[
                                       SourceTableName
                                   ],
                                  "jdbcUrl":[
                                       SourceJdbc
                                            ]
                                          }
                                        ]
                                      }
                                   },
                "writer": {
                    "name": DestinationName,
                    "parameter": {
                    "column":Column,
                    "connection":[
                          {
                                 "jdbcUrl":DestinationJDBC,
                    "table":[DestinationTableName]
                          }
                            ],

                    "writeMode":WriteMode,
                    "password":DestinationPassword,
                    "username":DestinationUser
                    }
                }
            }
        ],
       "setting":{
          "speed":{
             "channel":20
             }
       },
       "errorLimit":{
           "percentage":ErrorRecode
         }
    }
}
    jsonstr = json.dumps(dict, indent=4)
    return (jsonstr)
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    SourceUser = "root"
    SourcePassword = "Csy990309@"
    SourceName = "mysqlreader"
    DestinationUser = "root"
    DestinationPassword = "Csy990309@"
    DestinationName = "mysqlwriter"
    ErrorRecode = 0.01
    SourceTableName = "T_STU"
    splitPk = ""
    #SourceJdbc="jdbc:oracle:thin:@101.34.179.142:7010:orcl"
    SourceJdbc = "jdbc:mysql://101.34.179.142:3306/dataxweb"
    DestinationJDBC="jdbc:mysql://101.34.179.142:3306/dataxweb"
    Column=[]
    PkColumn=""
    DestinationTableName=''
    WriteMode="replace"
    if SourceName == 'oraclereader':
        Column,PkColumn=GetOracleColumn(SourceJdbc, SourceTableName, SourceUser, SourcePassword)
    elif SourceName == 'mysqlreader':
        Column=GetMysqlColumn(SourceJdbc, SourceTableName, SourceUser, SourcePassword)
    if DestinationName=='mysqlwriter' and SourceName=='oraclereader':
        DestinationTableName=(SourceTableName.split('.'))[1]
    else:
        DestinationTableName=SourceTableName
    JsonResult = MakeJson(SourceUser, SourcePassword, SourceName, DestinationUser, DestinationPassword, DestinationName,
                      ErrorRecode, SourceTableName,  SourceJdbc, DestinationJDBC,Column,DestinationTableName,PkColumn,WriteMode)
    print(JsonResult)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
