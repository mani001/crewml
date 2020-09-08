import pandas as pd

import calendar
import datetime
import pytz
from io import StringIO
import urllib.request
import os.path

'''
This program reads flight data, and converts all the local timestamp to UTC and removes
unnecessary data. Each row contains a specific flight

FlightDate	- Fligh Departure Date
Tail_Number	- Unique registeration number that identifies a equipment, given by FAA
Origin	 - Origin Airport
Dest	 - Destination Airport
DepTime  - Local Departure Time in hours and minutes 	
ArrTime	 - Local Arrival Time in hours and minutes
Distance - Distance in miles between Depature and Arrival airports

'''

'''
Function reads flight data from public S3 bucket
'''
def read_Flights_From_S3():
    data_path=os.path.dirname(__file__)+'/'
          
    #Jan 2020 flight data for Delta Airlines 
    flights_df = pd.read_csv(data_path+"flights_2020_jan_dl-Copy.csv")
    flights_df= flights_df.dropna()
    #create unique id for each flight (row)
    flights_df.insert(0, "fltID",flights_df.reset_index().index)
    
    #Read timezones for airport iata_code
    tz_df = pd.read_csv(data_path+"timezones.csv")
    
    return flights_df, tz_df


'''
Calculate TimeZones for the origin and destination local time for the flights
'''
def calculateTimeZone(x,y):
    temp1=(tz_df.loc[tz_df['iata_code'] == x, 'iana_tz']).values
    temp2=(tz_df.loc[tz_df['iata_code'] == y, 'iana_tz']).values
    
    if(temp1.size == 0):
        print("org'",x)
        
    if(temp2.size ==0):
        print("dest'",y)

    return temp1[0],temp2[0]


'''
This function calculates flight time and converts local departure and arrival time to UTC for
flight
'''
def calculateFltTime(x,y,z,tz1,tz2):
        
    timezone1 = pytz.timezone(tz1)
    timezone2 = pytz.timezone(tz2)
    
    print ("x=",x,"y=",y,"z=",z,"tz1=",tz1,"tz2=",tz2)
    flt_dt = datetime.datetime.strptime(x, "%m/%d/%Y")

    y=str(int(y)).zfill(4)
    z=str(int(z)).zfill(4)
    y_hour=int(y[0:2])
    y_min=int(y[2:4])
    z_hour=int(z[0:2])
    z_min=int(z[2:4])    
    if y_hour > 1:
        y_hour=y_hour-1
    if z_hour > 1:
        z_hour=z_hour-1    
    
    dt1= timezone1.localize(datetime.datetime(flt_dt.year,flt_dt.month, flt_dt.day, y_hour,y_min, 0, 0))
    dt2= timezone2.localize(datetime.datetime(flt_dt.year,flt_dt.month, flt_dt.day, z_hour,z_min, 0, 0))
    
    dt1_utc=dt1.astimezone(pytz.utc) 
    dt2_utc=dt2.astimezone(pytz.utc) 
    
        
    tot_days=calendar.monthrange(flt_dt.year, flt_dt.month)[1]
    if dt1 > dt2  :     
        if tot_days == flt_dt.day :
            month=flt_dt.month+1
            day=1
        else:
            month=flt_dt.month
            day=flt_dt.day+1 
        dt2= timezone2.localize(datetime.datetime(flt_dt.year,month, day, z_hour, z_min, 0, 0)) 
        dt2_utc=dt2.astimezone(pytz.utc) 
    
    flt_time=(dt2-dt1) 
    
    return flt_time, dt1_utc, dt2_utc 


    
#Read data from S3 public bucket    
flights_df, tz_df =read_Flights_From_S3()

#find timeZones for departure and arrival flight time
flights_df["DepTz"], flights_df["ArrTz"] = zip(*flights_df.apply(lambda x: calculateTimeZone(x['Origin'], x['Dest']), axis=1))

#cacluate flight time and convert orgion and destination local time to UTC
flights_df["FltTime"], flights_df["OrgUTC"], flights_df["DestUTC"] = zip(*flights_df.apply(lambda x: calculateFltTime(x['FlightDate'], x['DepTime'], x['ArrTime'],x["DepTz"],x["ArrTz"]), axis=1))

flights_df.to_csv("flights_cleaned.csv")
