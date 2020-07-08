import sys
import boto3
import pandas as pd
import glob
import os

#bucket='maas-poc-raw'
bucket='covid-19-dataset-20200628'
csv_folder = 'csse_covid_19_daily_reports/'
parq_folder = 'csse_covid_19_daily_reports_parquet/'
csv_data_location = 's3://{}/{}'.format(bucket,csv_folder )
parq_data_location = 's3://{}/{}'.format(bucket,parq_folder)

rename_map= {"Last Update" :"Last_Update", "Province/State" :  "Province_State", "Country/Region" :  "Country_Region"   }
cols = ["Province_State", "Country_Region", "Last_Update", "Active", "Confirmed", "Recovered", "Incidence_Rate"  ]
cols_default_vals = {'Active' : 0, 'Confirmed' : 0, 'Recovered' :0 }



s3 = boto3.resource('s3')
covid_bucket = s3.Bucket(bucket)

for obj in covid_bucket.objects.filter(Prefix=csv_folder):
    
    try :
        if '.csv' not in obj.key :
            continue
    
        file_path= csv_data_location+obj.key.split('/')[-1]
        df = pd.read_csv(file_path)
        df = df.rename(columns= rename_map)
        #df = df[cols]
        df.fillna(cols_default_vals, inplace = True)
        file_name = file_path.split('/')[-1][:-4]
        parq_path = parq_data_location+file_name+'.parquet'
        df.to_parquet(parq_path)
        print("file written")
        print(file_name)
    except Exception as ex:
         print (ex)
