import pandas as pd
import json

def lambda_handler(event,context):

    #import data files
    file1 = pd.read_csv("data_files/weather.20160201.csv")
    # file2 = pd.read_csv("data_files/weather.20160301.csv")

    #concat the data frames
    # df_csv = pd.concat([file1,file2],ignore_index=True)
    
    df_parquet_output = generate_result(file1)

    out_json = json.dumps(df_parquet_output)    

    return out_json

def generate_result(df):

    out_dict = {}
    
    df = df[(df.ScreenTemperature > -50.0) & (df.ScreenTemperature < 60.0)]
    df = df.drop_duplicates()

    # Export to parquet
    df.to_parquet('weather2016.parquet')

    # Reading parquet
    df_parquet = pd.read_parquet('weather2016.parquet')

    hottest_date = df_parquet.loc[df_parquet['ScreenTemperature'].idxmax()]

    print('-----------------------')
    temp_dict = {'Hottest date':pd.to_datetime(hottest_date['ObservationDate'])}
    out_dict.update(temp_dict)
    
    temp_dict = {'Temperature on that day':hottest_date['ScreenTemperature']}
    out_dict.update(temp_dict)
    
    temp_dict = {'On the region':hottest_date['Region']}
    out_dict.update(temp_dict)
    
    return out_dict