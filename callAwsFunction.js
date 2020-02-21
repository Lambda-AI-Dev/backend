var AWS = require('aws-sdk');

// AWS config details (TODO: Should be hidden in a credentials file)
// TODO: Replace with AWS config details

/**
 * Invoke a AWS Lambda function with particular name and inputs
 * 
 * @param functionName The AWS Lambda function name
 * @param input The inputs you want to pass into the AWS Lambda function
 * @param callback The callback function returning the response from the AWS Lambda Function
 */
function callAwsLambda (functionName, input, callback) {
    // Create Lambda object
    let lambda = new AWS.Lambda();
  
    // Create the Lambda function input parameters
    let params = { 
      FunctionName: functionName, 
      InvocationType: "RequestResponse", 
      LogType: "Tail",
      Payload: JSON.stringify(input)
    };
  
    // Invoke the Lambda function
    lambda.invoke(params, function(err, data) {
        callback(err, data);
    });
}

/**
 * Get a Multiple Choice image set to be labeled
 * 
 * @param labelerId The cookie fetched from the labeler
 */
function getMultipleChoiceImage(labelerId) {
    var input = {
        labelerId: labelerId
    };
    callAwsLambda("getMultipleChoiceImage", input, (err, data) => {
        if (err) {
            console.error('There was an error in invoking the function. See below.');
            console.error(err);
        } else {
            console.log('The data was successfully returned; see below.');
            console.log(data);
        }
    });
}

// No labelerId passed in, function will return with written warning.
getMultipleChoiceImage(null);

// Call 'getMultipleChoiceImage' AWS Lambda function with a labelerId.
getMultipleChoiceImage("48I3LLdAM52opOGC");