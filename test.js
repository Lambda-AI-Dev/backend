let crypto = require("crypto");

let AWS = require('aws-sdk');
require('dotenv').config();

// AWS config details
AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION,
  accessKeyId: process.env.AWS_INPUT_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_INPUT_SECRET_ACCESS_KEY
});

// Create DocumentClient object to allow methods to interact with DynamoDB database
let docClient = new AWS.DynamoDB.DocumentClient();

function myTest() {
  let tasks = [1, 2, 3, 4, 5];
  let promises = [];
  tasks.forEach((task) => {
    promises.push(new Promise((resolve, reject) => reject(new Error('Shoot.'))));
  });
  return new Promise((resolve, reject) => {
    Promise.all(promises).then(() => {
      resolve();
    }).catch((error) => {
      reject(error);
    });
  });
  

}

// myTest().then(() => {
//   console.log('I resolved!');
// }).catch((error) => {
//   console.log('I have an error.');
//   console.log(error);
// });

const id = crypto.randomBytes(16).toString("hex");
console.log(id);