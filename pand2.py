#importing needed libraries
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import warnings
from decimal import Decimal, getcontext

#SET PRECISION TO 2
getcontext().prec = 2

#IGNORE WARNINGS
warnings.filterwarnings("ignore")

#from datetime import datetime
#Extract data from db and convert in to dataframe
engine = create_engine('postgresql://postgres:postgres@localhost:5432/prac')
df = pd.read_sql('elec', con=engine)
#Normalize the consumption column that's in JSON format
df2=pd.json_normalize(df['consumption'])
df=df.join(df2)
del df['consumption']
df_for_indi_MPANS={}
for name in df['mpan'].value_counts().index.to_list():
    df_for_indi_MPANS[name]=df[df["mpan"]==name]
#print(df_for_indi_MPANS)
#print(df.info())

#1. At what time of the day (hour or half-hour slot) does each meter record the highest consumption?
for key, value in df_for_indi_MPANS.items():
    print("MPAN: ", key)
    #Get kWh columns
    keys=value.columns[2:]
    values=np.nanmean(value.iloc[:, 2:], axis=0)
    time_power_avg=dict(zip(keys, values))
    print(f"Time Period with Max Consumption is {max(time_power_avg, key=time_power_avg.get)}\n")

#2. How does consumption differ between weekdays and weekends for each meter?
#Has to be after 2024-02-05 as some MPANS only started recording consumption on this date
for key, value in df_for_indi_MPANS.items():
    value['day_of_week']=value['consumptiondate'].dt.weekday
    weekdays=[0, 1, 2, 3, 4]
    weekends=[5, 6]
    weekdays_df=value[value['day_of_week'].isin(weekdays)]
    weekends_df=value[value['day_of_week'].isin(weekends)]
    print(key)
    print(f"Average Weekday Consumption: {np.sum(np.nanmean(weekdays_df.iloc[:, 2:], axis=0)):.2f}")
    print(f"Average Weekend Consumption: {np.sum(np.nanmean(weekends_df.iloc[:, 2:], axis=0)):.2f}")
    print("\n")

#3. How does the total consumption of each meter compare (take any one available month)?
#Taking month as March, aka 3
for key, value in df_for_indi_MPANS.items():
    march_df=value[value['consumptiondate'].dt.month==3]
    print(key)
    print(f"Total Consumption for March: {np.sum(np.sum(march_df.iloc[:, 2:])):.2f}")
    print("\n")
print("\n")
#4. Which are the top 5 meters with the highest total consumption and the bottom 5 meters with the lowest total consumption?
meter_cons={}
for key, value in df_for_indi_MPANS.items():
    meter_cons[key]=np.sum(np.sum(value.iloc[:, 2:]))
print(meter_cons)
meter_cons_df=pd.DataFrame({'MPAN':meter_cons.keys(), 'Total_Cons':meter_cons.values()})
#meter_cons_df.sort_values('Total_Cons', inplace=True)
print("Top 5 highest Consumption: ")
print(meter_cons_df.sort_values('Total_Cons', ascending=False).head())
print("\n")
print("Top 5 least Consumption: ")
print(meter_cons_df.sort_values('Total_Cons', ascending=True).head())
print("\n")

#5. What are the 7-day rolling average consumption values for each meter,
#and on which days does consumption exceed 2× the rolling average (spike detection)?
for key, value in df_for_indi_MPANS.items():
    value['totalcons']=np.sum(value.iloc[:, 2:], axis=1)
    value['rolling_avg']=value['totalcons'].rolling(window=7).mean()
    #print(value)
    print(key)
    for index, row in value.iterrows():
        if (not np.isnan(row['rolling_avg'])):
            if(row['totalcons']>2*row['rolling_avg']):
                print(f"On {row['consumptiondate']}, the consumption {row['totalcons']:.2f} was more than two times the rolling avg {row['rolling_avg']:.2f}")
    print("\n")

#6. On which days did each meter record the lowest consumption, and what were those values?
for key, value in df_for_indi_MPANS.items():
    print(key)
    min_df=value[value['totalcons']==np.min(value['totalcons'])]
    print(f"Day with least consumption was {min_df.iloc[0, 1]} with consumption {min_df.iloc[0, 51]:.2f}")
    print("\n")

#7. Which meters have similar usage patterns (find correlation between meters)?
#Comparing usage on 01/01/2025
for key, value in df_for_indi_MPANS.items():
    print(key)
    x=value[value['consumptiondate']=='2025-01-01']
    #print(dict(x.iloc[:,2:50]).values())
    for key2, value2 in df_for_indi_MPANS.items():
        y= value2[value2['consumptiondate']=='2025-01-01']
        #correlation_coefficient = np.corrcoef(list(dict(x.iloc[:,2:50]).values()), list(dict(y.iloc[:,2:50]).values()))
        x1=x.iloc[:,2:50].values.tolist()
        y1=y.iloc[:,2:50].values.tolist()
        correlation_coefficient = np.corrcoef(x1[0], y1[0])
        if (correlation_coefficient[0, 1]>0.7 and correlation_coefficient[0, 1]<0.9):
            print(f"{key} and {key2} have a similar correlation of {correlation_coefficient[0, 1]:.2f}")
        
    print("\n")

#8. On which days does each meter show abnormally high or low consumption compared to its average (anomaly detection)?
#
for key, value in df_for_indi_MPANS.items():
    p25=np.percentile(value['totalcons'], 25)
    p75=np.percentile(value['totalcons'], 75)
    iqr=p75-p25
    print(key)
    for index, row in value.iterrows():
        if(row['totalcons']<(p25-(1.5*iqr))):
            print(f"On {row['consumptiondate']}, {row['totalcons']:.2f} was a lower outlier")
        if(row['totalcons']>(p75+(1.5*iqr))):
            print(f"On {row['consumptiondate']}, {row['totalcons']:.2f} was a higher outlier")
    print("\n")

#9. What is the load factor (average load ÷ peak load × 100) of each meter,
#  and which meters have the lowest load factor (poor utilization)?
load_factors={}
for key, value in df_for_indi_MPANS.items():
    avg_load=np.nanmean(value['totalcons'])
    peak_load=np.max(value['totalcons'])
    load_factors[key]=avg_load/peak_load*100
print(load_factors)
print("\n")
print(f"{min(load_factors, key=load_factors.get)} has least load factor of {min(load_factors.values()):.2f}")
