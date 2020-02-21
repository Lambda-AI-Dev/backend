exports.handler = async (event, context, callback) => {
    var datasetId = event.datasetId;
    // TODO: There are other keys submitted, including:
    // multiclass, imageId, classNameGiven, timestamp
    const response = {
        statusCode: 200,
    };
    if (datasetId == null) {
        response.body = JSON.stringify('Hello from Lambda! No datasetId' +
        ' object was provided; send in JSON with datasetId as a key.');
    } else {
        response.body = JSON.stringify('Hello from Lambda! datasetId: '
        + datasetId);
    }
    return response;
};
