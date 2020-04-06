let AWS = require('aws-sdk');
require('dotenv').config();

// AWS config details
AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION,
  accessKeyId: process.env.AWS_INPUT_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_INPUT_SECRET_ACCESS_KEY
});

// Create DocumentClient object to allow methods to interact with DynamoDB database
let docClient = new AWS.DynamoDB.DocumentClient();

/**
 * Get the task details (type, taskCount, maxClasses, maxOccurrences)
 * for that particular developerId
 * 
 * @param developerId the developer that requested the task
 */
function getTaskDetails(developerId) {
  return new Promise((resolve, reject) => {
    // Initialize params to scan 'developer_profile' table
    let params = {
      TableName: 'developer_profile',
      Key: {
        'developerId': developerId
      }
    }

    // Retrieve the task details set up for that developerId
    docClient.get(params, (error, data) => {
      if (!error) {
        resolve(data);
      } else {
        error.note = 'The get operation for the \'developer_profile\' table failed.';
        reject(error);
      }
    });
  });
}

getTaskDetails('5445029971295084').then((data) => {
  console.log('The request was successful, here is the data:');
  console.log(data);
}).catch((error) => {
  console.error('There was an error here.');
  console.error(error);
});