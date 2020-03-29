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

// NOTES: maxClasses overrides maxOccurrences, and if there are not enough tasks to satisfy
// type and taskCount a shorter list is returned

// Define constants (these would be set in a game developer console)
const type = 'text'; // Type of task given
const taskCount = 2; // Number of tasks given in this batch
const maxClasses = 5; // Number of classes to be given in testing
const maxOccurrences = 5; // Number of times a class should be give in a task

// NOTE: If 'labelerId' has more than 100 tasks currently in database associated with his
// profile, it will not get the rest

// TODO: Clean up tasks associated with a 'labelerId'

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
 * ...
 * 
 * @param data 
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

function exportsHandler (event) {
  // Define inputs
  let labelerId = event.labelerId;

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
        resolve(tasks);
      }).catch((error) => {
        reject(error);
      });
    }).catch((error) => {
      reject(error);
    });
  });
};

let event = {
  'labelerId': '5307751900195447'
}

exportsHandler(event).then((data) => {
  console.log(data);
}).catch((error) => {
  console.log(error);
});