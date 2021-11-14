import pandas as pd
import json

def lambda_handler(event,context):

    #import data files
    file1 = pd.read_csv("data_files/weather.20160201.csv")
    file2 = pd.read_csv("data_files/weather.20160301.csv")

    #concat the data frames
    df_csv = pd.concat([file1,file2],ignore_index=True)
    # print(df_csv.head())

    df_parquet_output = generate_result(df_csv)

    out_json = json.dumps(df_parquet_output)    

    return out_json

def generate_result(df):

    out_dict = {}
    # Remove duplicates
    df = df[(df.ScreenTemperature > -50.0) & (df.ScreenTemperature < 60.0)]
    df = df.drop_duplicates()

    # print(df_csv.describe())

    # Export to parquet
    df.to_parquet('weather2016.parquet')

    # Importing parquet
    df_parquet = pd.read_parquet('weather2016.parquet')

    # Question 1: The hottest date
    hottest_date = df_parquet.loc[df_parquet['ScreenTemperature'].idxmax()]

    print('-----------------------')
    # print('Question 1: Which date was the hottest day?')
    # print('Hottest date:', pd.to_datetime(hottest_date['ObservationDate']))
    temp_dict = {'Hottest date':pd.to_datetime(hottest_date['ObservationDate'])}
    out_dict.update(temp_dict)
    # Question 2: Temperature
    # print('-----------------------')
    # # print('Question 2: What was the temperature on that day?')
    # # print('Temperature on that day:', hottest_date['ScreenTemperature'])
    temp_dict = {'Temperature on that day':hottest_date['ScreenTemperature']}
    out_dict.update(temp_dict)
    # # Question 3: Region
    # print('-----------------------')
    # print('Question 3: In which region was the hottest day?')
    # print('On the region:', hottest_date['Region'])
    temp_dict = {'On the region':hottest_date['Region']}
    out_dict.update(temp_dict)
    
    return out_dict