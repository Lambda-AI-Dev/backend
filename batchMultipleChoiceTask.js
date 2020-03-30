let AWS = require('aws-sdk');

// AWS config details
AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION,
  accessKeyId: process.env.AWS_INPUT_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_INPUT_SECRET_ACCESS_KEY
});

// Create DocumentClient object to allow methods to interact with DynamoDB database
let docClient = new AWS.DynamoDB.DocumentClient();

// NOTE: maxClasses overrides maxOccurrences, and if there are not enough tasks to satisfy
// type and taskCount a shorter list is returned

// Define constants (these would be set in a game developer console)
const type = process.env.TYPE; // Type of task given
const taskCount = process.env.TASK_COUNT; // Number of tasks given in this batch
const maxClasses = process.env.MAX_CLASSES; // Number of classes to be given in testing
const maxOccurrences = process.env.MAX_OCCURRENCES; // Number of times a class should be give in a task

// NOTE: If 'labelerId' has more than 100 tasks currently in database associated with his
// profile, it will not get the rest

/**
 * Get the list of taskIds
 * 
 * @param labelerId
 * @param type 
 */
function getLabelerTaskList(labelerId, type) {
  let params = {
    TableName: 'labeler_task',
    FilterExpression: '#labelerId = :labelerId AND #type = :type',
    ExpressionAttributeNames: {
      '#labelerId': 'labelerId',
      '#taskId': 'taskId',
      '#type': 'type'
    },
    ProjectionExpression: '#taskId',
    ExpressionAttributeValues: {
      ':labelerId': labelerId,
      ':type': type
    }
  }

  // Scan 'labeler_task' table for tasks of 'type' already completed by 'labelerId'
  return new Promise((resolve, reject) => {
    docClient.scan(params, (error, data) => {
      if (!error) {
        resolve(data);
      } else {
        error.note = 'The scan operation for the \'labeler_task\' table failed.';
        reject(error);
      }
    });
  });  
}

/**
 * Get the multiple choice tasks to complete with 'type' for 'labelerId'
 * This function is recursive, re-calling the 'scan' function until it reaches
 * 'taskCount' number of valid tasks
 * 
 * @param taskParams params for the scan call to the 'unfinished_task' table
 * @param labelerId the 'labelerId' for which these tasks are requested
 * @param taskCount the number of tasks requested for 'labelerId'
 * @param tasks the list of task objects to be returned 
 */
function getTasks(taskParams, labelerId, taskCount, tasks) {
  // Recursive promise iteration
  return new Promise((resolve, reject) => {
    // Scan 'unfinished_task' table
    docClient.scan(taskParams, (error, data) => {
      if (!error) {
        // Structure the tasks, and add to the 'tasks' variable
        structureTasks(data, labelerId).forEach((task) => {
          tasks.push(task);
        });
        // If tasks have not yet reached 'taskCount', recurse
        if (tasks.length < taskCount && data.LastEvaluatedKey != null) {
          taskParams.ExclusiveStartKey = data.LastEvaluatedKey;
          resolve(getTasks(taskParams, labelerId, taskCount, tasks));
        } else {
          resolve(tasks);
        }
      } else {
        error.note = 'The scan operation for the \'unfinished_task\' table failed.';
        reject(error);
      }
    });
  });
}

/**
 * Format the tasks with the correct number of classes and adding the 
 * necessary data (including particular components to be populated upon 
 * task completion)
 * 
 * @param data the data returned from scanning the 'unfinished_task' table for new tasks
 * @param labelerId the 'labelerId' for which these tasks are requested
 */
function structureTasks(data, labelerId) {
  // Construct return object
  let tasks = [];
      
  // Iterate through available tasks
  data.Items.forEach((item) => {
    let index = 0;
    let total = Object.entries(item.class).length;

    let classes = {};
    Object.keys(item.class).forEach((className, occurrences) => {
      // Automatically add class if there are not enough remaining
      // OW, add if class needs to be given out more (and maxClasses hasn't been reached)
      if ((total - index) + Object.entries(classes).length <= maxClasses) {
        classes[className] = false;
      } else if (occurrences < maxOccurrences && Object.entries(classes).length < maxClasses) {
        classes[className] = false;
      }
      index++;
    });

    // Add the 'labelerId' to 
    item.labelerId = labelerId;

    // Replace current 'class' variable with the classes to be tested
    item.class = classes;

    // Set other variables specific to the task
    item.jobId = (Math.random() + '').substring(2,10) 
     + (Math.random() + '').substring(2,10);
    item.labelingMethod = 'multipleChoice';
    item.stoppedByTimer = null;
    item.beginTimestamp = null;
    item.endTimestamp = null;

    // Add item to 'tasks', to be returned
    tasks.push(item);
  });

  return tasks;
}

function get(labelerId, response) {
  return new Promise((resolve, reject) => {
    getLabelerTaskList(labelerId, type).then((data) => {
      // Initialize params to scan 'unfinished_task' table
      let params = {
        TableName: 'unfinished_task',
        Limit: taskCount,
        ProjectionExpression: 'instructions, #taskId, multiclass, #data, #class, #type',
        ExpressionAttributeNames: {
          '#taskId': 'taskId',
          '#type': 'type',
          '#data': 'data',
          '#class': 'class'
        }
      }
      
      // Initialize taskObject to provide dynamically-produced ExpressionAttributeValues
      let taskObject = {};

      // If 'labelerId' was not previously in table, do not include 'taskId' in FilterExpression
      if (data.Count == 0) {
        params.FilterExpression = '#type = :type';
      } else {
        // Define 'taskObject' to filter results to not contain already-given tasks
        let index = 0;
        data.Items.forEach((item) => {
          taskObject[(':taskId' + index)] = item.taskId;
          index++;
        });
        // Update params to filter 'taskId' accordingly
        params.FilterExpression = 'NOT ( #taskId IN (' + Object.keys(taskObject).toString()
          + ') ) AND #type = :type';
      }

      // Assign value mappings for FilterExpression
      taskObject[':type'] = type;
      params.ExpressionAttributeValues = taskObject;

      // Scan 'unfinished_task' table for tasks of 'type' and not previously given to 'labelerId'
      getTasks(params, labelerId, taskCount, []).then((tasks) => {
        response.body = JSON.stringify(tasks);
        resolve(response);
      }).catch((error) => {
        reject(error);
      });
    }).catch((error) => {
      reject(error);
    });
  });
}

function post(tasks) {
  return new Promise((resolve, reject) => {
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
                  } else {
                    // Resolve to indicate finished function
                    resolve();
                  }
                });
              } else if (!error) {
                // Resolve to indicate finished function
                resolve();
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
                error.note = 'The put operation for the \'unfinished_task\' table failed.';
                reject(error);
              } else {
                // Resolve to indicate finished function
                resolve();
              }
            });

          }

        } else {
          error.note = 'The get operation for the \'unfinished_task\' table failed.';
          reject(error);
        }

      });

    });

  });
}

exports.handler = async (request) => {
  // Define response
  let response = {
    headers: {},
    body: '',
    statusCode: 200
  }

  return new Promise((resolve, reject) => {
    if (request.httpMethod == 'GET') {
      get(request.pathParameters.labelerId, response).then((taskResponse) => {
        resolve(taskResponse);
      }).catch((error) => {
        reject(error);
      });
    } else if (request.httpMethod == 'POST') {
      post(JSON.parse(request.body)).then(() => {
        response.body = 'Success.';
        resolve(response);
      }).catch((error) => {
        reject(error);
      });
    }
  });
};