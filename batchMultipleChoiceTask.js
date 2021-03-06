let AWS = require('aws-sdk');
let crypto = require('crypto');

// AWS config details
AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION,
  accessKeyId: process.env.AWS_INPUT_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_INPUT_SECRET_ACCESS_KEY
});

// Create DocumentClient object to allow methods to interact with DynamoDB database
let docClient = new AWS.DynamoDB.DocumentClient();

// Const set for as environment variable maxOccurrences
// NOTE: maxClasses overrides maxOccurrences
// TODO: This may be specified in the 'task', later
const maxOccurrences = process.env.MAX_OCCURRENCES;

// NOTE: If 'labelerId' has more than 100 tasks currently in database associated with his
// profile, it will not get the rest

/**
 * Get the list of taskIds
 * 
 * @param labelerId the labelerId for which to get valid tasks
 * @param type the type of tasks to retrieve
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
function getTasks(taskParams, labelerId, taskCount, maxClasses, tasks) {
  // Recursive promise iteration
  return new Promise((resolve, reject) => {
    // Scan 'unfinished_task' table
    docClient.scan(taskParams, (error, data) => {
      if (!error) {
        // Structure the tasks, and add to the 'tasks' variable
        structureTasks(data, labelerId, maxClasses).forEach((task) => {
          tasks.push(task);
        });
        // If tasks have not yet reached 'taskCount', recurse
        if (tasks.length < taskCount && data.LastEvaluatedKey != null) {
          taskParams.ExclusiveStartKey = data.LastEvaluatedKey;
          resolve(getTasks(taskParams, labelerId, taskCount, maxClasses, tasks));
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
 * @param maxClasses the maximum number of classes that should be given
 */
function structureTasks(data, labelerId, maxClasses) {
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
    item.jobId = crypto.randomBytes(8).toString('hex');
    item.labelingMethod = 'multipleChoice';
    item.skipped = false;
    item.stoppedByTimer = null;
    item.beginTimestamp = null;
    item.endTimestamp = null;

    // Add item to 'tasks', to be returned
    tasks.push(item);
  });

  return tasks;
}

// NOTE: If there are not enough tasks to satisfy type and taskCount a shorter list is returned

/**
 * Get the task details (type, taskCount, maxClasses)
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

function get(labelerId, developerId, response) {
  return new Promise((resolve, reject) => {
    // Get type, taskCount, maxClasses from developerId
    getTaskDetails(developerId).then((taskDetailData) => {
      getLabelerTaskList(labelerId, taskDetailData.Item.type).then((data) => {
        // Initialize params to scan 'unfinished_task' table
        let params = {
          TableName: 'unfinished_task',
          Limit: taskDetailData.Item.taskCount,
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
        taskObject[':type'] = taskDetailData.Item.type;
        params.ExpressionAttributeValues = taskObject;
  
        // Scan 'unfinished_task' table for tasks of 'type' and not previously given to 'labelerId'
        getTasks(params, labelerId, taskDetailData.Item.taskCount, 
         taskDetailData.Item.maxClasses, []).then((tasks) => {
          // Add in developerId for each task
          tasks.forEach((task) => {
            task.developerId = developerId;
          })
          response.body = JSON.stringify(tasks);
          resolve(response);
        }).catch((error) => {
          reject(error);
        });
      }).catch((error) => {
        reject(error);
      });
    }).catch((error) => {
      reject(error);
    });

  });
}

/**
 * Upload the task to the 'job' and 'labeler_task' tables
 * 
 * @param task a single completed task
 */
function uploadBaseTask(task) {
  return new Promise((resolve, reject) => {
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
        'taskId': task.taskId,
        'developerId': task.developerId,
        'skipped': task.skipped
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
      } else {
        // Do not update any more tables if task was skipped.
        resolve();
      }
    });
  });
}

/**
 * Update the count of classes completed in 'task' database
 * If applicable, update the 'dataset' databases to indicate 
 * finished tasks, including moving the task itself
 * 
 * NOTE: This should not be called if task.skipped == true
 * 
 * @param task a single completed task
 */
function updateTaskDataset(task) {

  // Return a Promise to indicate success / failure
  return new Promise((resolve, reject) => {
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
              error.note = 'The update operation for the \'unfinished_task\' table failed.';
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
  
}

/**
 * Post the list of labeled tasks
 * 
 * @param tasks list of labeled tasks from user
 */
function post(tasks) {

  // List of promises to verify that all tasks pass
  let promises = [];
  
  // Iterate over tasks
  tasks.forEach((task) => {
    promises.push(new Promise((resolve, reject) => {
      // Upload the task to 'job' and 'labeler_task' tables
      uploadBaseTask(task).then(() => {
        // If the user skipped the task, 
        if (task.skipped) {
          resolve();
        }
      }).catch((error) => {
        reject(error);
      });

      // If task is not skipped, further update dataset
      if (!task.skipped) {
        updateTaskDataset(task).then(() => {
          resolve();
        }).catch((error) => {
          reject(error);
        });
      }
      
    }));

  });

  // Verify that all of the tasks were uploaded correctly;
  // otherwise, return an error
  return new Promise((resolve, reject) => {
    Promise.all(promises).then(() => {
      resolve();
    }).catch((error) => {
      reject(error);
    });
  });

}

exports.handler = async (request) => {
  // Define response
  let response = {
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Credentials': true,
    },
    body: '',
    statusCode: 200
  }

  return new Promise((resolve, reject) => {
    if (request.httpMethod == 'GET') {
      get(request.pathParameters.labelerId, request.pathParameters.developerId, 
       response).then((taskResponse) => {
        resolve(taskResponse);
      }).catch((error) => {
        response.body = JSON.stringify(error);
        resolve(response);
      });
    } else if (request.httpMethod == 'POST') {
      post(JSON.parse(request.body).results).then(() => {
        response.body = 'Success.';
        resolve(response);
      }).catch((error) => {
        response.body = JSON.stringify(error);
        resolve(response);
      });
    }
  });
};