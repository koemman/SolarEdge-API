import urllib.request
import datetime
from datetime import timedelta, datetime, date
import dateutil.parser
from influxdb import InfluxDBClient    
import sys
import secrets as sc
import pandas as pd 



filename="energy"
#site parameters
siteID = sc.siteID # add site id from Solaredge portal
invID = sc.invID  # string inverter ID  from Solaredge portal
apiKey = sc.apiKey  # api KEY from Solaredge portal
filePath = sc.inputpath +"/" + filename + ".csv"  # path used to download data
outPath = sc.outputpath +"/" + filename + ".csv" # path used to export the processed data
server_address=sc.influx_server
server_port=sc.influx_port
db=sc.database


#connection to influx
client=InfluxDBClient(host=server_address,port=server_port)
#print(client.get_list_database())
client.switch_database(db)

#query database to get the datetime of latest entry
results=client.query('SELECT last(value), time FROM site_energy')
lastpoint=list(results.get_points(measurement='site_energy'))
stday=str(dateutil.parser.parse(lastpoint[0]['time']).date())


yesterday=date.today()+timedelta(days=-1)
enday = yesterday.strftime("%Y-%m-%d")


##url of the webservice
url='https://monitoringapi.solaredge.com/site/'+str(siteID)+'/energy.csv?api_key='+apiKey+'&timeUnit=DAY&endDate='+enday+'&startDate='+stday
try:
    urllib.request.urlretrieve(url, filePath)  # request data from url  and save file in filePath.
except Exception as e:
    print(str(e))

##url of the webservice
url='https://monitoringapi.solaredge.com/site/'+str(siteID)+'/energy.xml?api_key='+apiKey+'&timeUnit=DAY&endDate='+enday+'&startDate='+stday
##Processing the data

#read_csv to dataframe
data=pd.read_csv(filePath)
data.dropna(subset=['date'],inplace=True) #delete empty rows(without date)

#convert datetime to ISO 8601 format for insertion to influx
data['date']=data['date'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S').isoformat())

#convert columns with float and integers to float, otherwise it gives error when inserting integer to float column
#conv_dict is a dictionary with column names and float
#conv_dict={'temperature' : float, 'dcVoltage' : float,'groundFaultResistance' : float,'vL1To2' : float,'vL2To3' : float,'vL3To1' : float,'L1-acCurrent' : float,'L1-acVoltage' : float,'L1-acFrequency' : float,'L2-acCurrent' : float,'L2-acVoltage' : float,'L2-acFrequency' : float,'L2-apparentPower' : float,'L2-activePower' : float,'L2-reactivePower' : float,'L3-acCurrent' : float,'L3-acVoltage' : float,'L3-acFrequency' : float}
#data = data.astype(conv_dict)


# add a column inverter_data to be used as the name of the measurement table 
data['inverter_data']='site_energy'
# add a column with inv_id as (tag field)
data['site_id']=sc.siteID

#print(data.head())
#print(data.dtypes)

#export panda to csv file for insertion to influx
data.to_csv(outPath,index=False, float_format='%.3f')