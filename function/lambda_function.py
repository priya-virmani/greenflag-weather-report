import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import io

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

# insert_data function for inserting data into dynamodb table
def insert_data(recDict):
    table = dynamodb.Table('weatherReport') # pylint: disable=E1101
    print("Put items into DynamoDB")
    table.put_item(
            Item={
                'Year': recDict.get('Year'),
                'Month': recDict.get('Month'),
                'Date': recDict.get('Hottest date'),
                'Temperature': repr(recDict.get('Temperature')),
                'Region': recDict.get('Region'),
            }
        )
        
def write_pandas_parquet_to_s3(df, bucketName, keyName,):
    
    # dummy dataframe
    print("in write_pandas_parquet_to_s3")
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)
        
    # upload to s3
    s3_client.put_object(Body=out_buffer.getvalue(), Bucket=bucketName, Key=keyName)
    
    in_buffer = io.BytesIO()
    client = boto3.resource('s3')
    obj = client.Object(bucketName,keyName)
    obj.download_fileobj(in_buffer)
    
    return in_buffer
    
    
def generate_result(df):

    out_dict = {}
    
    df = df[(df.ScreenTemperature.astype(str) > '-50.0') & (df.ScreenTemperature.astype(str) < '60.0')]
    df = df.drop_duplicates()

    # Export to parquet
    print("export df to parquet file")
    in_buffer = write_pandas_parquet_to_s3(df, "all-lambda-code-deploy-bucket", "data_files/parquet/weatherReport.parquet")
    
    # Reading parquet
    df_parquet = pd.read_parquet(in_buffer)

    hottest_date = df.loc[pd.to_numeric(df['ScreenTemperature']).idxmax()]
    
    print('-----------------------')
    temp_dict = {'Year':pd.to_datetime(hottest_date['ObservationDate']).year}
    out_dict.update(temp_dict)

    temp_dict = {'Month':pd.to_datetime(hottest_date['ObservationDate']).month}
    out_dict.update(temp_dict)
    
    temp_dict = {'Hottest date':pd.to_datetime(hottest_date['ObservationDate']).to_pydatetime().date().strftime("%Y-%m-%d")}
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
        
        #Get S3 object
        response = s3_client.get_object(Bucket = bucket, Key = key)
        
        # #Read CSV File
        csv_df = pd.read_csv(response.get("Body"))
        
        #Fetching the data from Dataframe
        df_parquet_output = generate_result(csv_df)
        
        #Putting the items into DynamoDB
        insert_data(df_parquet_output)

    except Exception as e:
        print(e)
