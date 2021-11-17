# greenflag-weather-report
## Table of Contents
* [General Info](#general-info)
* [Rquirements](#requirements)
* [Technologies](#technologies)
* [Steps](#Steps)

## General Info
This project is a serverless application to generate a Weather Report.

## Requirements
Convert the weather data into parquet format. Set the row group to the appropriate value you see fit for this data The converted data should be able to answer the following question:

1. Which date was the hottest day?
2. What was the temperature on that day?
3. In which region was the hottest day?

## Technologies
This project is created with:
* Python-3.9
* AWS - S3, Lambda, CloudWatch, DynamoDB, CloudFormation, CodePipeline.

## Steps
To run this project:
```
1. Upload a CSV weather File to S3 bucket.
2. It will trigger the lambda function to process the data.
3. The lambda will consume the csv dataset and convert it into parquet format.
4. The process will find out the hottest day, temperature and region details and store them into DynamoDB in weatherReport table.
```
