#importing needed libraries
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import warnings

#IGNORE WARNINGS
warnings.filterwarnings("ignore")

#from datetime import datetime
#1. Extract data from db and convert in to dataframe
engine = create_engine('postgresql://postgres:postgres@localhost:5432/prac')
df = pd.read_sql('elec', con=engine)


print("\n")

#2. Basic Exploration (View & Inspect Data ,Check Missing Values,Filtering )
print("First 5 rows of dataframe:")
print(df.head())
print("\n")
print("Information on DataFrame:")
print(df.info())
print("\n")
print("NaN value Count:")
print(df.isna().value_counts())
print("\n")
print("Various MPANS")
print(df['mpan'].value_counts())

#3. Total consumption  of  each meter

#Empty dictionary that will contain dataframes corresponding to each MPAN
dict={}
for name in df['mpan'].value_counts().index.to_list():
    dict[name]=df[df["mpan"]==name]
print(dict)

#Function that sums up all the values in a single JSON object and then adds it to a global
#variable called 'total', which will contain the total consumption for each MPAN
def sums(df_row):
    global total
    total = total + sum(list(df_row.values()))

print("\nTotal Consumption for each MPAN")
for key, value in dict.items():
    total=0
    meter=value['consumption']
    #print(type(meter.iloc[0]))
    meter.apply(sums)
    print(f"MPAN: {key}, Total Consumption: {total}")

#4. On which day of the week is consumption very high? (for every meter)
days={0: "Monday", 1:"Tuesday", 2:"Wednesday", 3:"Thursday", 4:"Friday", 5:"Saturday", 6:"Sunday"}
#Going through each day of the week and finding power consumption
for key, value in dict.items():
    print(f"\nMPAN: {key}")
    #value['consumptiondate']=pd.to_datetime(value['consumptiondate'])
    #convert date into weekday
    value['consumptionday']=value['consumptiondate'].dt.weekday
    #print(value)
    #day_power stores the power consumption for each day of the week for a particular MPAN
    day_power={}
    for i in [0, 1, 2, 3, 4, 5, 6]:
        total=0
        temp=value[value['consumptionday']==i]
        #print(temp)
        temp['consumption'].apply(sums)
        print(f"Day of Week: {days[i]}, Total Consumption: {total}")
        day_power[i]=total
    print(f"Day of the Week with Most Consumption is {days[max(day_power, key=day_power.get)]}")

#5. What is the monthly consumption  each meter ?  (Take 2024 data) 
#6. Take particular year from the data, and then find for each meter which month energy consumption is very high   

#Nearly the same as 4
months={1:"January", 2:"February", 3:"March", 4:"April", 5:"May", 6:"June",
        7:"July", 8:"August", 9:"September", 10:"October", 11:"November", 12:"December"}
for key, value in dict.items():
    print(f"\nMPAN: {key}")
    #value['consumptiondate']=pd.to_datetime(value['consumptiondate'])
    value['consumptionmonth']=value['consumptiondate'].dt.month
    value['consumptionyear']=value['consumptiondate'].dt.year
    #print(value)
    month_power={}
    for i in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
        total=0
        temp=value[value['consumptionyear']==2024]
        temp=temp[temp['consumptionmonth']==i]
        temp['consumption'].apply(sums)
        print(f"Month: {months[i]}, Total Consumption: {total}")
        month_power[i]=total
    print(f"Month with Most Consumption is {months[max(month_power, key=month_power.get)]}") 