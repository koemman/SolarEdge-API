#####-----------------------------#####
#####IMPORT XML FILE INTO DATABASE#####

from urllib.request import urlopen
from xml.etree.ElementTree import parse
import mysql.connector
import sys

#site parameters
siteID = 1234 # add site id from Solaredge portal

#mysql server connection settings
mysql_host='192.168.1.15'
mysql_user= 'user'
mysql_pass= 'pass'
mysql_db='pv_db'



##CONNECT TO DATABASE

conn = mysql.connector.Connect(host=mysql_host, user=mysql_user,password=mysql_pass, database=mysql_db)

c = conn.cursor()


##DEFINE XML FILEPATH AND PARSE IT
u = 'C:\PythonPVfile\ppppp.xml'
doc = parse(u)

##SQL CODE TO ADD ENTRY IN TABLE
add_record = ("INSERT INTO invdata "
              "(Date, PAC, VDC,Temp,IL1,VL1,FL1,IL2,VL2,FL2,IL3,VL3,FL3,SN) "
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

# PROCESS XML FILE AND ENTER RECORDS IN TABLE

try:

    for item in doc.iterfind('telemetries/threePhaseInverterTelemetry'):
        date = item.findtext('date')
        pdc = item.findtext('totalActivePower')
        vdc = item.findtext('dcVoltage')
        temp = item.findtext('temperature')
        il1 = item.findtext('L1Data/acCurrent')
        vl1 = item.findtext('L1Data/acVoltage')
        fl1 = item.findtext('L1Data/acFrequency')
        il2 = item.findtext('L2Data/acCurrent')
        vl2 = item.findtext('L2Data/acVoltage')
        fl2 = item.findtext('L3Data/acFrequency')
        il3 = item.findtext('L3Data/acCurrent')
        vl3 = item.findtext('L3Data/acVoltage')
        fl3 = item.findtext('L3Data/acFrequency')
        sn = siteID

        data_record = (date, pdc, vdc, temp, il1, vl1, fl1, il2, vl2, fl2, il3, vl3, fl3, sn)

        c.execute(add_record, data_record)
        conn.commit()
except Exception as e:
    print(str(e))

c.close()
conn.close()

#####--------------------------------------#####