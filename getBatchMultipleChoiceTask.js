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

exports.handler = async (event, context, callback) => {

  // Define inputs
  let taskCount = event.taskCount;
  let labelerId = event.labelerId;
  let type = event.type;

  // Scan 'labeler_task' table for tasks of 'type' already completed by 'labelerId'
  let params = {
    TableName: 'labeler_task',
    FilterExpression: '#labelerId = :labelerId AND #type = :type',
    ExpressionAttributeNames: {
      '#labelerId': 'labelerId',
      '#type': 'type'
    },
    ExpressionAttributeValues: {
      ':labelerId': labelerId,
      ':type': type
    }
  }

  // TODO: Address 'lastEvaluatedKey', if needed

  // Scan 'labeler_task' table
  docClient.scan(params, (error, data) => {
    if (!error) {
      // Define 'taskObject' to filter results to not contain already-given tasks
      let taskObject = {};
      let index = 0;
      for (item in data.Items) {
        taskObject[('taskId' + index)] = item.taskId;
        index++;
      }
      taskObject[':type'] = type;

      // Scan 'task' table for tasks of 'type' and not previously given to 'labelerId'
      let params = {
        TableName: 'task',
        Limit: taskCount,
        FilterExpression: 'NOT ( #taskId IN (' + Object.keys(taskObject).toString()
         + ') ) AND #type = :type',
        ExpressionAttributeNames: {
          '#taskId': 'taskId',
          '#type': 'type'
        },
        ExpressionAttributeValues: taskObject
      }

      // Scan 'task' table
      docClient.scan(params, (error, data) => {
        if (!error) {

        } else {
          console.error(error);
        }
      });

    } else {
      console.log(err);
    }
  });

  scanFilteredItemsFromDatabase('dataset', taskCount, '#finished = :')

  let labelerId = event.labelerId;
  let promise = new Promise((resolve, reject) => {
    if (labelerId == null) {
      resolve(JSON.stringify('Hello from Lambda! No Labeler ID was' +
      ' provided; send in JSON with labelerId as a key.'));
    } else if (labelerId == '48I3LLdAM52opOGC') {
      // TODO: This will replaced with searching through the database for
      // a particular labelerId
      resolve(JSON.stringify('Hello from Lambda! Labeler ID: '
      + labelerId));
    } else {
      reject(JSON.stringify('Labeler ID is not in database.'));
    }
  });
  return promise;
};










/**
 * Get a Multiple Choice task set to be labeled
 *
 * @param taskCount The number of tasks requested
 * @param labelerId The cookie fetched from the labeler
 * @param type The type of task to return (currently only text)
 */
function getBatchMultipleChoiceTask(taskCount, labelerId, type) {
  let input = {
    taskCount: taskCount,
    labelerId: labelerId,
    type: type
  };

  let response = {
    jobId: 'g1JjD3mYtPi0jk7z',
    datasetId: 'de2uutrbhzz72kx5',
    taskId: 'evGixcimEj5e1VTP',
    classes: ['Positive', 'Negative', 'Neutral'],
    multiclass: false,
    type: 'text',
    data: 'I am Chris.',
    instruction: 'Choose the appropriate sentiment for this text.'
  }


  return new Promise((resolve, reject) => {
    callAwsLambda('getMultipleChoiceTask', input).then((data) => {
      console.log(data); // TODO: For debugging
      resolve(data);
    }).catch((err) => {
      reject(err);
    });
  });
}

// No labelerId passed in, function will return with written warning.
getMultipleChoiceTask(null);

// Call 'getMultipleChoiceTask' AWS Lambda function with a labelerId.
getMultipleChoiceTask('48I3LLdAM2opOGC').then((response) => {
  console.log(typeof(response.Payload));
})
