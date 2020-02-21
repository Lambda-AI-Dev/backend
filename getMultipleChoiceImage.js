exports.handler = async (event, context, callback) => {
    var labelerId = event.labelerId;
    
    const response = {
        statusCode: 200,
    };
    if (labelerId == null) {
        response.body = JSON.stringify('Hello from Lambda! No Labeler ID was' +
        ' provided; send in JSON with labelerId as a key.');
    } else {
        response.body = JSON.stringify('Hello from Lambda! Labeler ID: '
        + labelerId);
    }
    return response;
};
