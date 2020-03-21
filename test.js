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

// Define constants
const maxClasses = 5; // Number of classes to be given in testing
const maxOccurrences = 5; // Number of times a class should be given in a task

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

// NOTE: If 'labelerId' has more than 100 tasks currently in database associated with his
// profile, it will not get the rest

// TODO: Clean up tasks associated with a 'labelerId'

// Scan 'labeler_task' table
let promise = new Promise((resolve, reject) => {
  docClient.scan(params, (error, data) => {
    if (!error) {
      // Define 'taskObject' to filter results to not contain already-given tasks
      let taskObject = {};
      let index = 0;
      data.Items.forEach((item) => {
        taskObject[(':taskId' + index)] = item.taskId;
        index++;
      });
  
      // Scan 'task' table for tasks of 'type' and not previously given to 'labelerId'
      let params = {
        TableName: 'task',
        Limit: taskCount,
        FilterExpression: 'NOT ( #taskId IN (' + Object.keys(taskObject).toString()
          + ') ) AND #type = :type AND #progress < :one',
        ProjectionExpression: 'instructions, datasetId, taskId, multiclass, #data, #class, #type',
        ExpressionAttributeNames: {
          '#progress': 'progress',
          '#taskId': 'taskId',
          '#type': 'type',
          '#data': 'data',
          '#class': 'class'
        }
      }
  
      // Assign value mappings for FilterExpression
      taskObject[':type'] = type;
      taskObject[':one'] = 1;
      params.ExpressionAttributeValues = taskObject;
  
      // Scan 'task' table
      docClient.scan(params, (error, data) => {
        if (!error) {
          // Construct return object
          let tasks = [];
          
          // Iterate through available tasks
          data.Items.forEach((item) => {
            let index = 0;
            let total = Object.entries(item.class).length;
  
            let classes = {};
            Object.keys(item.class).forEach((className, occurrences) => {
              // Automatically add class if there are not enough remaining
              // OW, add if class needs to be given out more
              if ((total - index - 1) + Object.entries(classes).length <= maxClasses) {
                classes[className] = false;
              } else if (occurrences < maxOccurrences) {
                classes[className] = false;
              }
            });
  
            // Replace current 'class' variable with the classes to be tested
            item.class = classes;
  
            // Set other variables specific to the task
            item.labelingMethod = 'multipleChoice';
            item.stoppedByTimer = null;
            item.beginTimestamp = null;
            item.endTimestamp = null;
  
            // Add item to 'tasks', to be returned
            tasks.push(item);
          });
  
          resolve(tasks);
        } else {
          error.note = 'The scan operation for the \'task\' table failed.';
          reject(error);
        }
      });
    } else {
      error.note = 'The scan operation for the \'labeler_task\' table failed.';
      reject(error);
    }
  });
});

promise.then((data) => {
  console.log(data);
}).catch((error) => {
  console.log(error);
});