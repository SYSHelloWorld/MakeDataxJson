# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import json
import sys
import cx_Oracle
import pymysql
#获取mysql字段，mysql不获取pk，无增速效果
def GetMysqlColumn(SourceJdbc,TableName,SourceUser,SourcePassword):
    #将jdbc进行分割获得ip，port，dbname
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
    cursor.close()
    conn.close()
    return Column
#获取oracle字段和pk
def GetOracleColumn(SourceJdbc, TableName, SourceUser, SourcePassword):
    TableName = TableName.split('.')  #将oracle表分割成owner和tablename
    JdbcSplit=SourceJdbc.split(':')
    Ip=JdbcSplit[3].replace('@','')
    dns_tns=cx_Oracle.makedsn(Ip,JdbcSplit[4],sid=JdbcSplit[5])
    conn = cx_Oracle.connect(SourceUser,SourcePassword,dns_tns)
    cur = conn.cursor()
    Column=[]
    PkColumn=""
    #获取columnname
    sql = "select column_name from all_tab_columns where table_name =\'" + TableName[1] + "\' and owner=\'" + TableName[
        0] + "\'"
    cur.execute(sql)
    result = cur.fetchall()
    for row in result:
        Column.append((row[0]))
    #获取pk
    sql = "select cu.column_name from all_cons_columns cu,all_constraints au where cu.CONSTRAINT_NAME= au.CONSTRAINT_NAME and cu.owner=au.owner and au.constraint_type = 'P' and au.TABLE_NAME=\'"+ TableName[1] + "\'and cu.owner =\'" + TableName[0] +"\'"
    cur.execute(sql)
    PkResult=cur.fetchall()
    PkColumn=PkResult[0]
    cur.close()
    conn.close()
    return Column,PkColumn
#获取pg字段和库
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
    with open("./"+SourceTableName+".json",'w') as write_f:
        write_f.write(json.dumps(dict, indent=4))
    jsonstr = json.dumps(dict, indent=4)
    return jsonstr
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    SourceUser = sys.argv[1]           #源端账号
    print(SourceUser)
    SourcePassword = sys.argv[2]       #源端密码
    SourceName = sys.argv[3]           #源端数据库类型
    SourceJdbc = sys.argv[4]           #源端jdbc
    DestinationUser = sys.argv[5]      #目标端账号
    DestinationPassword = sys.argv[6]  #目标端密码
    DestinationName = sys.argv[7]      #目标端数据库类型
    DestinationJDBC = sys.argv[8]      #目标端jdbc
    ErrorRecode = sys.argv[9]          #错误率往往在0-1
    SourceTableName = sys.argv[10]     #目标端表
    WriteMode = sys.argv[11]           #写入模式
    splitPk = ""
    Column = []
    PkColumn = ""
    DestinationTableName = ''
    #将表进行分割
    SourceTableName=SourceTableName.split(',')
    for SourceTableName in SourceTableName:
        # SourceJdbc="jdbc:oracle:thin:@101.34.179.142:7010:orcl"
        if SourceName == 'oraclereader':
            Column, PkColumn = GetOracleColumn(SourceJdbc, SourceTableName, SourceUser, SourcePassword)
        elif SourceName == 'mysqlreader':
            Column = GetMysqlColumn(SourceJdbc, SourceTableName, SourceUser, SourcePassword)
        #将表名进行切割形成目标库端表名，这里可以目标段表名格式
        if DestinationName == 'mysqlwriter' and SourceName == 'oraclereader':
            DestinationTableName = (SourceTableName.split('.'))[1]
        else:
            DestinationTableName = SourceTableName
        JsonResult = MakeJson(SourceUser, SourcePassword, SourceName, DestinationUser, DestinationPassword,
                              DestinationName,
                              ErrorRecode, SourceTableName, SourceJdbc, DestinationJDBC, Column, DestinationTableName,
                              PkColumn, WriteMode)
        print(JsonResult)



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
