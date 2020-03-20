let AWS = require('aws-sdk');
require('dotenv').config();

// AWS config details
AWS.config.update({
  region: process.env.REGION,
  accessKeyId: process.env.ACCESS_ID,
  secretAccessKey: process.env.SECRET_KEY
});

/**
 * Invoke a AWS Lambda function with particular name and inputs
 *
 * @param functionName The AWS Lambda function name
 * @param input The inputs you want to pass into the AWS Lambda function
 */
function callAwsLambda(functionName, input) {
  // Create Lambda object
  let lambda = new AWS.Lambda();

  // Create the Lambda function input parameters
  let params = {
    FunctionName: functionName,
    InvocationType: 'RequestResponse',
    LogType: 'Tail',
    Payload: JSON.stringify(input)
  };

  // Invoke the Lambda function
  return new Promise((resolve, reject) => {
    lambda.invoke(params, (err, data) => {
      if (!err) {
        resolve(data);
      } else {
        // Create a new Error object
        reject(new Error(err));
      }
    });
  });
}

// TODO: Below this is done.





