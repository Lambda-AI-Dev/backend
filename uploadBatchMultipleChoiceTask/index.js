let AWS = require('aws-sdk');

// AWS config details
AWS.config.update({
  region: process.env.REGION,
  accessKeyId: process.env.ACCESS_ID,
  secretAccessKey: process.env.SECRET_KEY
});

// Create DocumentClient object to allow methods to interact with DynamoDB database
let docClient = new AWS.DynamoDB.DocumentClient();

// Define constants
const maxOccurrences = 5; // Number of times a class should be given in a task

exports.handler = async (event) => {
  // Define inputs
  let tasks = event.tasks;

  return new Promise((_, reject) => {
    // Iterate over tasks
    tasks.forEach((task) => {

      // Add to 'job' table
      let jobParams = {
        TableName: 'job',
        Item: {
          'labelerId': task.labelerId,
          'jobId': task.jobId,
          'beginTimestamp': task.beginTimestamp,
          'class': task.class,
          'endTimestamp': task.endTimestamp,
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
          'jobId': task.jobId,
          'taskId': task.taskId,
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

      // Get class, progress, and datasetId from 'unfinished_task' table
      let taskGetParams = {
        TableName: 'unfinished_task',
        Key: {
          'taskId': task.taskId
        }
      }
      docClient.get(taskGetParams, (error, data) => {
        if (!error) {

          // Use task and data to update class and progress
          Object.keys(task.class).forEach((className) => {
            data.Item.class[className]++;
            // If class is finished, update task progress
            if (data.Item.class[className] == maxOccurrences) {
              // Update progress
              data.Item.progress.current++;
            }
          });

          // TODO: This should never be '>', but what if it is?

          // If task is finished, move task from 'unfinished_task' table to 'finished_task' table,
          // and update both 'dataset' tables
          if (data.Item.progress.current == data.Item.progress.total) {

            // Create new item in 'finished_task' table
            data.TableName = 'finished_task';
            docClient.put(data, (error, _) => {
              if (error) {
                // TODO: How would this operation be amended?
                error.note = 'The put operation for the \'finished_task\' table failed.';
                reject(error);
              }
            });

            // Remove item in 'unfinished_task' table
            let finishedTaskParams = {
              TableName: 'unfinished_task',
              Key: {
                'taskId': task.taskId
              }
            }
            docClient.delete(finishedTaskParams, (error, _) => {
              if (error) {
                // TODO: How would this operation be amended?
                error.note = 'The delete operation for the \'unfinished_task\' table failed.';
                reject(error);
              }
            });

            // Update 'finished' status of the 'taskId' in 'dataset_<DATASETID>' table
            let datasetFinishedTaskParams = {
              TableName: 'dataset_' + data.Item.datasetId,
              Key: {
                'taskId': task.taskId
              },
              UpdateExpression: 'SET #finished = :true',
              ExpressionAttributeNames: {
                '#finished': 'finished'
              },
              ExpressionAttributeValues: {
                ':true': true
              },
              ReturnValues: 'UPDATED_NEW'
            }
            docClient.update(datasetFinishedTaskParams, (error, _) => {
              if (error) {
                // TODO: How would this operation be amended?
                error.note = 'The update operation for the \'dataset_' + data.Item.datasetId 
                 + '\' table failed.';
                reject(error);
              }
            });

            // Update current 'progress' counter in 'dataset' table
            let datasetProgressParams = {
              TableName: 'dataset',
              Key: {
                'datasetId': '4898691044887699'
              },
              UpdateExpression: 'SET #progress.#current = #progress.#current + :one',
              ExpressionAttributeNames: {
                '#progress': 'progress',
                '#current': 'current'
              },
              ExpressionAttributeValues: {
                ':one': 1
              },
              ReturnValues: 'UPDATED_NEW'
            }
            docClient.update(datasetProgressParams, (error, data) => {
              if (!error && data.Item.progress.current == data.Item.progress.total) {
                // Conditionally update 'finished' status in 'dataset' table
                let datasetFinishedParams = {
                  TableName: 'dataset',
                  Key: {
                    'datasetId': '4898691044887699'
                  },
                  UpdateExpression: 'SET #finished = :true',
                  ExpressionAttributeNames: {
                    '#finished': 'finished',
                    '#total': 'total'
                  },
                  ExpressionAttributeValues: {
                    ':true': true
                  },
                  ReturnValues: 'UPDATED_NEW'
                }
                docClient.update(datasetFinishedParams, (error, _) => {
                  if (error) {
                    error.note = 'The second update operation for the \'dataset\' table failed.';
                    reject(error);
                  }
                });
              } else {
                error.note = 'The second update operation for the \'dataset\' table failed.';
                reject(error);
              }
            });

          } else {

            // Update 'unfinished_task' table
            let taskUpdateParams = {
              TableName: 'unfinished_task',
              Key: {
                'taskId': task.taskId
              },
              UpdateExpression: 'SET #class = :class, #progress = :progress',
              ExpressionAttributeNames: {
                '#class': 'class',
                '#progress': 'progress'
              },
              ExpressionAttributeValues: {
                ':class': data.Item.class,
                ':progress': data.Item.progress
              },
              ReturnValues: 'UPDATED_NEW'
            }
            docClient.update(taskUpdateParams, (error, data) => {
              if (error) {
                // TODO: How would this operation be amended?
                error.note = 'The put operation for the \'unfinished_task\' table failed.';
                reject(error);
              }
            });

          }

        } else {
          // TODO: How would this operation be amended?
          error.note = 'The get operation for the \'unfinished_task\' table failed.';
          reject(error);
        }

      });

    });

  });
};