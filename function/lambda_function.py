import pandas as pd
import boto3

s3_client = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

# insert_data function for inserting data into dynamodb table
def insert_data(recDict):
    table = dynamodb.Table('weatherReport') # pylint: disable=E1101
    table.put_item(
            Item={
                'Year': recDict.get('Year'),
                'Month': recDict.get('Month'),
                'Hottest Date': recDict.get('Hottest date'),
                'Temperature': recDict.get('Temperature'),
                'Region': recDict.get('Region'),
            }
        )
    
def generate_result(df):

    out_dict = {}
    
    df = df[(df.ScreenTemperature > -50.0) & (df.ScreenTemperature < 60.0)]
    df = df.drop_duplicates()

    # Export to parquet
    df.to_parquet('weatherReport.parquet')

    # Reading parquet
    df_parquet = pd.read_parquet('weather2016.parquet')

    hottest_date = df_parquet.loc[df_parquet['ScreenTemperature'].idxmax()]
    
    print('-----------------------')
    temp_dict = {'Year':pd.to_datetime(hottest_date['ObservationDate']).year}
    out_dict.update(temp_dict)

    temp_dict = {'Month':pd.to_datetime(hottest_date['ObservationDate']).month}
    out_dict.update(temp_dict)

    temp_dict = {'Hottest date':pd.to_datetime(hottest_date['ObservationDate'])}
    out_dict.update(temp_dict)
    
    temp_dict = {'Temperature':hottest_date['ScreenTemperature']}
    out_dict.update(temp_dict)
    
    temp_dict = {'Region':hottest_date['Region']}
    out_dict.update(temp_dict)
    
    return out_dict

def lambda_handler(event,context):

    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        csv_file = s3_client.get_object(Bucket = bucket, Key = key)
        
        #import data files
        # file1 = pd.read_csv("data_files/weather.20160201.csv")
        # file2 = pd.read_csv("data_files/weather.20160301.csv")
        # file1 = pd.read_csv(csv_file)

        #concat the data frames
        # df_csv = pd.concat([file1,file2],ignore_index=True)
        
        df_parquet_output = generate_result(csv_file)
        # print(df_parquet_output)
        
        insert_data(df_parquet_output)

    except Exception as e:
        print(e)
