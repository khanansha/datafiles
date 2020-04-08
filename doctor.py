import mysql.connector
import json 
import pandas as pd 
from pandas.io.json import json_normalize
import json
from sqlalchemy import create_engine

import pymysql

import pandas as pd
#with open('C:/Users/akhan.extern/Desktop/jsondata/doctors.json',encoding="utf8") as json_data:
    #sample_object = json.load(json_data)

#df = pd.json_normalize(sample_object)
with open('C:/Users/akhan.extern/Desktop/jsondata/doctors.json',encoding="utf8") as f:
    data = json.load(f)

def validate_string(val):
   if val != None:
        if type(val) is int:
            #for x in val:
            #   print(x)
            return str(val).encode('utf-8')
        else:
            return val

#print(data)   
con = pymysql.connect(host = 'localhost',user = 'root',passwd = '',db = 'doctors') 
cursor = con.cursor()
#print(con)  
# parse json data to SQL insert
#The enumerate() function takes a collection (e.g. a tuple) and returns it as an enumerate object.
for i, item in enumerate(data):
    userId = validate_string(item.get("userId", None))
    fullName = validate_string(item.get("fullName", None))
    gender = validate_string(item.get("gender", None))
    phoneNumber = validate_string(item.get("phoneNumber", None))
    #zoyloCharge = validate_string(item.get("zoyloCharge", None))
    zoyloRating = validate_string(item.get("zoyloRating", None))
    serviceProviderName = validate_string(item.get("serviceProviderName", None))
    homeVisitFlag = validate_string(item.get("homeVisitFlag", None))

    cursor.execute("INSERT INTO doctors (userId,fullName,gender,phoneNumber,zoyloRating,serviceProviderName,homeVisitFlag) VALUES (%s,%s,%s,%s,%s,%s,%s)", (userId,fullName,gender,phoneNumber,zoyloRating,serviceProviderName,homeVisitFlag))
con.commit()
con.close()



#tableName   = "details"
#sqlEngine = create_engine('mysql+pymysql://root:@127.0.0.1/test', pool_recycle=3600)

#dbConnection    = sqlEngine.connect()
#try:

    #frame = df.to_sql(tableName, dbConnection, if_exists='fail')

#except ValueError as vx:

   # print(vx)

#except Exception as ex:   

   # print(ex)

#else:

  #  print("Table %s created successfully."%tableName);   

#finally:

   # dbConnection.close()


#con = mysql.connector.connect(
#user="root",
#passwd="",
#database="doctors"
#)
#with open('C:/Users/akhan.extern/Desktop/jsondata/doctors.json',encoding="utf8") as json_data:
   # sample_object = json.load(json_data)

#df = pd.io.json.json_normalize(sample_object)
#df.columns = df.columns.map(lambda x: x.split(".")[-1])

#print(df.shape)
#print(con)        
#Insert whole DataFrame into MySQL
#df.to_sql(con=con, name='test', if_exists='replace')                


