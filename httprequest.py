import urllib.request
import datetime
import mysql.connector
import sys

#site parameters
siteID = 1234 # add site id from Solaredge portal
invID = 'XXXXXXXX-65'  # string inverter ID  from Solaredge portal
apiKey = 'XXXXXXXXXXXXXXXXXXXXXXXX'  # api KEY from Solaredge portal
filePath = 'C:\PythonPVfile\ppppp.xml'  # path used to store data

#mysql server connection settings
mysql_host='192.168.1.15'
mysql_user= 'user'
mysql_pass= 'pass'
mysql_db='pv_db'



##CONNECT TO DATABASE

conn = mysql.connector.Connect(host=mysql_host, user=mysql_user,password=mysql_pass, database=mysql_db)

c = conn.cursor()

##FIND LAST ENTRY

query = ("SELECT * FROM invdata ORDER BY DATE DESC LIMIT 1")

c.execute(query)

ldate = c.fetchone()  # the most recent datetime record

##preparing start date input for url
stdate = ldate[0] + datetime.timedelta(seconds=10)  # adding 10seconds to avoid redownloading the last record
stday = str(stdate.date())  # keeping just day
sttime = str(stdate.time())  # keeping just time

c.close()
conn.close()

##------------------##

##preparing last day input for url
i = datetime.datetime.now()
enday = str(i.date())  # keeping just day

##url of the webservice
url = 'https://monitoringapi.solaredge.com/equipment/' + str(
    siteID) + '/' + invID + '/data.xml?api_key=' + apiKey + '&startTime=' + stday + '%20' + sttime + '&endTime=' + enday + '%2023:59:59'

try:
    urllib.request.urlretrieve(url, filePath)  # request data from url  and save file in filePath.
except Exception as e:
    print(str(e))

import importxml2mysql








