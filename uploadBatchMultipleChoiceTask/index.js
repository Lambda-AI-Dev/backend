let AWS = require('aws-sdk');

// AWS config details
AWS.config.update({
  region: process.env.REGION,
  accessKeyId: process.env.ACCESS_ID,
  secretAccessKey: process.env.SECRET_KEY
});

// Create DocumentClient object to allow methods to interact with DynamoDB database
let docClient = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
  // Define inputs
  let tasks = event.tasks;

  return new Promise((resolve, reject) => {
    // Iterate over tasks
    tasks.forEach((task) => {
      // Update both dataset tables (conditionally), the job table, the task table, and the labeler_task table

      // Add to 'job' table
      let jobParams = {
        TableName: 'job',
        Item: {
          'jobId': task.jobId,
          'beginTimestamp': task.beginTimestamp,
          'class': task.class,
          'endTimestamp': task.endTimestamp,
          'labelerId': task.labelerId,
          'labelingMethod': task.labelingMethod,
          'stoppedByTimer': task.stoppedByTimer,
          'taskId': task.taskId
        }
      }
      docClient.put(jobParams, (error, _) => {
        if (error) {
          // TODO: How would this operation be amended?
          error.note = 'The put operation for the \'job\' table failed.';
          reject(error);
        }
      });

      // Add to 'labeler_task' table
      let labelerTaskParams = {
        TableName: 'labeler_task',
        Item: {
          'labelerId': task.labelerId,
          'taskId': task.labelerId,
          'type': task.type
        }
      }
      docClient.put(labelerTaskParams, (error, _) => {
        if (error) {
          // TODO: How would this operation be amended?
          error.note = 'The put operation for the \'labeler_task\' table failed.';
          reject(error);
        }
      });

      // Add to 'labeler_task' table
      let labelerTaskParams = {
        TableName: 'labeler_task',
        Item: {
          'labelerId': task.labelerId,
          'taskId': task.labelerId,
          'type': task.type
        }
      }
      docClient.put(labelerTaskParams, (error, _) => {
        if (error) {
          // TODO: How would this operation be amended?
          error.note = 'The put operation for the \'labeler_task\' table failed.';
          reject(error);
        }
      });

      // Get class, progress, and datasetId from 'task' table
      let taskGetParams = {
        TableName: 'task',
        Key: {
          'taskId': task.taskId
        }
      }
      docClient.get(taskGetParams, (error, data) => {
        if (!error) {
          let newClass = {}

          // Use task and data to update class and progress

          // Update 'task' table
          let taskUpdateParams = {
            TableName: 'task',
            Key: {
              'taskId': task.taskId
            },
            UpdateExpression: 'set ',
            ExpressionAttributeNames: {
              '#class': 'class'
            },
            ExpressionAttributeValues: {
              ':class': newClass
            },
            ReturnValues: 'UPDATED_NEW'
          }
          docClient.put(labelerTaskParams, (error, _) => {
            if (error) {
              // TODO: How would this operation be amended?
              error.note = 'The put operation for the \'task\' table failed.';
              reject(error);
            }
          });

          // If all tables are uploaded correctly, resolve
          resolve();
        } else {
          // TODO: How would this operation be amended?
          error.note = 'The get operation for the \'task\' table failed.';
          reject(error);
        }

      });

    });

  });
};
