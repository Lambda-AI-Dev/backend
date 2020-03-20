let AWS = require('aws-sdk');
require('dotenv').config();

// AWS config details
AWS.config.update({
  region: process.env.REGION,
  accessKeyId: process.env.ACCESS_ID,
  secretAccessKey: process.env.SECRET_KEY
});

// Create DocumentClient object to allow methods to interact with DynamoDB database
let docClient = new AWS.DynamoDB.DocumentClient();

/**
 * Query items from the DynamoDB database; used when the particular database has a 
 * composite key so the response will have multiple different database values
 * 
 * @param table The name of the table from which you are fetching data
 * @param partitionKeyName The name of the partition key (not the actual key value)
 * @param partitionKey The value of the primary/partition key you are searching for
 */
function queryItemsFromDatabase(table, partitionKeyName, partitionKey) {
  // Create the DynamoDB database 'query' input parameters
  let params = {
    TableName: table,
    KeyConditionExpression: '#key = :value',
    ExpressionAttributeNames:{
      '#key': partitionKeyName
    },
    ExpressionAttributeValues: {
      ':value': partitionKey
    }
  }
  
  console.log('queryItemsFromDatabase Entered! Table: ' + table + ' ' + partitionKeyName + ' ' + partitionKey);
  
  // Get the data entries based on the parameters defined above
  return new Promise((resolve, reject) => {
    docClient.query(params, (err, data) => {
      if (!err) {
        resolve(data);
      } else {
        reject(err);
      }
    });
  });
}

function scanFilteredItemsFromDatabase(table, limit, filterExpression) {
  // Create the DynamoDB database 'scan' input parameters
  let params = {
    TableName: table,
    Limit: limit,
    FilterExpression: filterExpression
  }

  // Get the data entries based on the parameters defined above
  return new Promise((resolve, reject) => {
    docClient.scan(params, (err, data) => {
      if (!err) {
        resolve(data);
      } else {
        reject(err);
      }
    });
  });
}