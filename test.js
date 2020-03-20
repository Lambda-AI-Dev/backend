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


// Define inputs
let taskCount = 2;
let labelerId = '6snPx3hh1MYgnaha';
let type = 'text';

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
    data.Items.forEach((item) => {
      console.log(item);
      taskObject[(':taskId' + index)] = item.taskId;
      index++;
    });

    // Scan 'task' table for tasks of 'type' and not previously given to 'labelerId'
    let params = {
      TableName: 'task',
      Limit: taskCount,
      FilterExpression: 'NOT ( #taskId IN (' + Object.keys(taskObject).toString()
        + ') ) AND #type = :type AND #progress < 1',
      ExpressionAttributeNames: {
        '#progress': 'progress',
        '#taskId': 'taskId',
        '#type': 'type'
      },
      ProjectionExpression: 'instructions, datasetId, taskId, multiclass, '
    }

    // Assign value mappings for FilterExpression
    taskObject[':type'] = type;
    params.ExpressionAttributeValues = taskObject;

    // Scan 'task' table
    docClient.scan(params, (error, data) => {
      if (!error) {
        console.log(data);
        // Construct return object
        let tasks = [];
        
        // Iterate through available tasks
        data.Items.forEach((item) => {
          let task = {};
          task.instructions = item.instructions;
          task.datasetId = item.datasetId;
        });
      } else {
        console.error(error);
      }
    });

  } else {
    console.log(err);
  }
})
