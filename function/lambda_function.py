import pandas as pd
import boto3
import csv
import pyarrow as pa
import pyarrow.parquet as pq
import awswrangler as wr

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
        
# def write_pandas_parquet_to_s3(df, bucketName, keyName, fileName):
#     # dummy dataframe
#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, fileName)

#     # upload to s3
#     BucketName = bucketName
#     with open(fileName) as f:
#       object_data = f.read()
#       s3_client.put_object(Body=object_data, Bucket=BucketName, Key=keyName)
    
def generate_result(df):

    out_dict = {}
    
    df = df[(df.ScreenTemperature.astype(str) > '-50.0') & (df.ScreenTemperature.astype(str) < '60.0')]
    df = df.drop_duplicates()

    # Export to parquet
    wr.pandas.to_parquet(dataframe=df,path="s3://all-lambda-code-deploy-bucket/data_files/parquet/weatherReport.parquet")
    # write_pandas_parquet_to_s3(df, "all-lambda-code-deploy-bucket", "data_files/parquet/weatherReport.parquet", "data_files/temp/file.parquet")
    # df.to_parquet('weatherReport.parquet')

    # Reading parquet
    df_parquet = pd.read_parquet('./tmp/weatherReport.parquet')

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

        response = s3_client.get_object(Bucket = bucket, Key = key)
        print("fetch s3 path")
        #import data files
        # file1 = pd.read_csv("data_files/weather.20160201.csv")
        # file2 = pd.read_csv("data_files/weather.20160301.csv")
        csv_reader = response["Body"].read().decode("utf-8")
        file1 = csv.reader(csv_reader.split('\r\n'))
        data=[]
        header = next(file1)
        for row in file1:
            data.append(row)
        df = pd.DataFrame(data=data, columns=header)
        # file1 = pd.read_csv(csv_file)
        print("creating dataframe")
        
        #concat the data frames
        # df_csv = pd.concat([file1,file2],ignore_index=True)

        df_parquet_output = generate_result(df)
        # print(df_parquet_output)
        
        insert_data(df_parquet_output)

    except Exception as e:
        print(e)
