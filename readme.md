# Auto Dyn - Table automation for AWS DynamoDB

This is sample code that demonstrates how to programmatically create dynamo tables
and load data to them. While this is demonstration code it can be modified to assist
in dynamically building dynamo resources for development purposes. 

Performs the following actions
- Create tables with indexes
- Load data into tables
- Delete tables

## Future
- Dump data to files
- Create user interface to manage actions and pointers to files
- Tie into S3 for storage


Note: You will need to supply your own AwsCredentials.properties file.
This sample is also built in java on top of vertx