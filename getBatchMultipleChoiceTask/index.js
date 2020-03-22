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
const maxClasses = 5; // Number of classes to be given in testing
const maxOccurrences = 5; // Number of times a class should be given in a task

// NOTE: If 'labelerId' has more than 100 tasks currently in database associated with his
// profile, it will not get the rest

// TODO: Clean up tasks associated with a 'labelerId'

/**
 * Get the list of taskIds
 * 
 * @param labeler_id
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
 * @param taskParams params for the scan call to the 'task' table
 * @param taskCount the number of tasks requested for 'labelerId'
 * @param tasks the list of task objects to be returned 
 */
function getTasks(taskParams, taskCount, tasks) {
  // Recursive promise iteration
  return new Promise((resolve, reject) => {
    // Scan 'task' table
    docClient.scan(taskParams, (error, data) => {
      if (!error) {
        // Structure the tasks, and add to the 'tasks' variable
        structureTasks(data).forEach((task) => {
          tasks.push(task);
        });
        // If tasks have not yet reached 'taskCount', recurse
        if (tasks.length < taskCount && data.LastEvaluatedKey != null) {
          taskParams.ExclusiveStartKey = data.LastEvaluatedKey;
          resolve(getTasks(taskParams, taskCount, tasks));
        } else {
          resolve(tasks);
        }
      } else {
        error.note = 'The scan operation for the \'task\' table failed.';
        reject(error);
      }
    });
  });
}

function structureTasks(data) {
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

exports.handler = async (event) => {
  // Define inputs
  let taskCount = event.taskCount;
  let labelerId = event.labelerId;
  let type = event.type;

  return new Promise((resolve, reject) => {
    getLabelerTaskList(labelerId, type).then((data) => {
      // Define 'taskObject' to filter results to not contain already-given tasks
      let taskObject = {};
      let index = 0;
      data.Items.forEach((item) => {
        taskObject[(':taskId' + index)] = item.taskId;
        index++;
      });

      // TODO: Handle case where 'labelerId' doesn't exist
  
      // Scan 'task' table for tasks of 'type' and not previously given to 'labelerId'
      let params = {
        TableName: 'task',
        Limit: taskCount,
        FilterExpression: 'NOT ( #taskId IN (' + Object.keys(taskObject).toString()
          + ') ) AND #type = :type AND #progress < :one',
        ProjectionExpression: 'instructions, taskId, multiclass, #data, #class, #type',
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

      getTasks(params, taskCount, []).then((tasks) => {
        resolve(tasks);
      }).catch((error) => {
        reject(error);
      });
    }).catch((error) => {
      reject(error);
    });
  });
};