var AWS = require('aws-sdk');

// AWS config details (TODO: Should be hidden in a credentials file)
// TODO: Replace with AWS config details

// Create DocumentClient object to allow methods to interact with DynamoDB database
var docClient = new AWS.DynamoDB.DocumentClient();

/**
 * Query items from the DynamoDB database; used when the particular database has a 
 * composite key so the response will have multiple different database values
 * 
 * @param table The name of the table from which you are fetching data
 * @param partitionKeyName The name of the partition key (not the actual key value)
 * @param partitionKey The value of the primary/partition key you are searching for
 * @param callback The callback function returning the response from the database
 */
function queryItemsFromDatabase(table, partitionKeyName, partitionKey, callback) {
    // Create the DynamoDB database 'query' input parameters
    let params = {
      TableName : table,
      KeyConditionExpression: "#key = :value",
      ExpressionAttributeNames:{
          "#key": partitionKeyName
      },
      ExpressionAttributeValues: {
          ":value": partitionKey
      }
    };
    
    console.log("queryItemsFromDatabase Entered! Table: " + table + " " + partitionKeyName + " " + partitionKey);
  
    // Get the data entries based on the parameters defined above
    docClient.query(params, function(err, data) {
        if (err) {
          console.error("Unable to query. Error: ", " ", table, " ", JSON.stringify(err, null, 2));
          callback(err);
        } else {
          console.log("Query succeeded. ", table);
          console.log(data);
          callback(data);
        }
    });
}

/**
 * Get the images that a particular labeler has already seen
 * 
 * @param labelerId The cookie fetched from the labeler
 */
function getLabeledImages(labelerId) {
    queryItemsFromDatabase("labeler_profile", "labelerId", labelerId, (err, data) => {
        if (err) {
            console.error('There was an error in fetching the data. See below.');
            console.error(err);
        } else {
            console.log('The data was successfully returned; see below.');
            console.log(data);
        }
    });
}

// Get the imageId list associated with a labelerId from DynamoDB.
getLabeledImages("hlqok9u9Bf1OtBG6");