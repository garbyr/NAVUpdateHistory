// dependencies
const aws = require('aws-sdk');
aws.config.update({ region: 'eu-west-1' });
var DV = require('./dateHelper.js');
//globals
var error = false;
var errorMessage = [];
var functionName = "";


exports.handler = (event, context, callback) => {
    //parse the event from SNS
    console.log(event);
    var dateSequence = new Date().getTime().toString();
    var dateTime = new Date().toUTCString();
    functionName=context.functionName;
    //execute the main process
    console.log("INFO: begin processing");
    //async call to get last sequence and callback to main
    getLastSequence(context, event, callback, dateSequence, dateTime, mainProcess);
}


mainProcess = function (context, input, callback, dateSequence, dateTime, lastSequence) {
    //calculate new, last and expected last sequence for the ISIN
    console.log("INFO: begin main process");
    var newSequence = createSequence(input.calculationDate, input.frequency, input.ISIN).toString();
    if (error) {
        raiseError((input.ISIN, newSequence, input.requestUUID, input.user, callback));
    }
    var expectedLastSequence = getExpectedLastSequence(newSequence, input.calculationDate);
    var sequenceFloor = parseInt(newSequence) - 500;
    //upload process may pass in a sequence - we may validate that the entries in the file (newSequence) match the sequenceIn
    var sequenceIn = input.sequence;
    console.log("INFO: sequence in " + sequenceIn);
    console.log("INFO: calculated equence " + newSequence);
    console.log("INFO: last sequence " + lastSequence);
    console.log("INFO: expected last sequence " + expectedLastSequence);
    console.log("INFO: sequence floor " + sequenceFloor);
    /*
     if (expectedLastSequence > LastSequence) {
         raiseError(ISIN, sequence, requestUUID, user, "Incorrect sequence for frequency " + frequency +" : the expected sequence > actual last sequence");
         context.fail();
         return;
     } else if (lastSequence < sequenceFloor) {
         raiseError(ISIN, sequence, requestUUID, user, "Incorrect sequence for frequency " + frequency +" : the sequence is outside the the ESMA calculation range");
         context.fail();
         return;
     }
     */
    //write to the database
    var dynamo = new aws.DynamoDB();
    var tableName = "NAVHistory";
    var item = {
        RequestUUID: { "S": input.requestUUID },
        ISIN: { "S": input.ISIN },
        NAV: { "N": input.NAV },
        Frequency: { "S": input.frequency },
        UpdatedTimeStamp: { "N": dateSequence },
        UpdatedDateTime: { "S": dateTime },
        UpdateUser: { "S": input.user },
        Sequence: { "S": newSequence },
        CalculationDate: {"S": input.calculationDate}
    }
    console.log(item);
    var params = {
        TableName: tableName,
        Item: item
    }
    console.log("INFO: writing NAV History record");
    dynamo.putItem(params, function (err, data) {
        if (err) {
            console.log("ERROR: writing NAV History record", err);
            error = true;
            errorMessage.push("NAV History not updated; error writing record to database");
            raiseError((input.ISIN, newSequence, input.requestUUID,input.user, callback));
        }
        else {
            console.log("SUCCESS: writing NAV History record", data);
            dynamoAudit(item, callback);    
        }
    });
}

dynamoAudit = function(item, callback){
    var dynamo = new aws.DynamoDB();
    var tableName = "NAVHistory";
    var item = {
        RequestUUID: { "S": input.requestUUID },
        ISIN: { "S": input.ISIN },
        NAV: { "N": input.NAV },
        Frequency: { "S": input.frequency },
        UpdatedTimeStamp: { "N": dateSequence },
        UpdatedDateTime: { "S": dateTime },
        UpdateUser: { "S": input.user },
        Sequence: { "S": newSequence },
        CalculationDate: {"S": input.calculationDate}
    }
    console.log(item);
    var params = {
        TableName: tableName,
        Item: item
    }
    console.log("INFO: writing NAV Audit record");
    dynamo.putItem(params, function (err, data) {
        if (err) {
            console.log("ERROR: writing NAV Audit record", err);
            error = true;
            errorMessage.push("NAV Audit not updated; error writing record to database");
            raiseError((input.ISIN, newSequence, input.requestUUID,input.user, callback));
        }
        else {
            console.log("SUCCESS: writing NAV Audit record", data);
                    
                var response = {
                    requestUUID: input.requestUUID,
                    ISIN: input.ISIN,
                    NAV: input.NAV,
                    sequence: newSequence,
                    frequency: input.frequency,
                    category: input.category,
                    user: input.user,
                }
                var output={
                    status: "200",
                    response: response
                }
                callback(null, {response});          
        }
    });
}

getLastSequence = function (context, input, callback, dateSequence, dateTime, _callback) {
    var dynamo = new aws.DynamoDB();
    var params = {
        TableName: 'NAVHistory',
        Limit: 1,
        ScanIndexForward: false,
        // Expression to filter on indexed attributes
        KeyConditionExpression: '#hashkey = :hk_val',
        ExpressionAttributeNames: {
            '#hashkey': 'ISIN',
        },
        ExpressionAttributeValues: {
            ':hk_val': { "S": ISIN },
        }
    };

    //may want to add additional filter here that includes frequency - just in case a single ISIN has two frequencies
    dynamo.query(params, function (err, data) {
        if (err) {
            console.log("ERROR", err);
            error = true;
            errorMessage.push("failed to retrieve latest  NAV record - update aborted");
            console.log("ERROR: failed to retrieve latest NAV record.  update request aborted");
            raiseError(ISIN, sequence, requestUUID, user, callback);
        }
        else {
       var lastSequence = data.items[0].Sequence;
       _callback(context, input, callback, dateSequence, dateTime, lastSequence);
}

getExpectedLastSequence = function (sequence) {
    var expectedLastSequence;
    var week = parseInt(sequence.substr(4, 2));
    if (week > 1) {
        week = week - 1;
        if (week < 9) {
            expectedLastSequence = (sequence.substr(0, 4) + "0" + week.toString());
        } else {
            expectedLastSequence = (sequence.substr(0, 4) + week.toString());
        }
    } else {
        var year = parseInt(sequence.substr(0,4));
        var expectedYear = (year - 1).toString();
        var weeksInYear = new DV.getWeeksInYearForYear(expectedYear);
        expectedLastSequence = expectedYear + weeksInYear;

    }
        return expectedLastSequence;
 }

raiseError = function (ISIN, sequence, requestUUID, user, callback) {
    //write to the database
     var errorObj = {
        requestUUID: requestUUID,
        ISIN:ISIN,
        sequence: sequence,
        user: user,
        function: functionName,
        messages: errorMessage,

    }
    //reset error details just in case container is reused!!
    error = false;
    errorMessage = [];
    callback(errorObj);
}

createSequence = function (dateIn, frequency, ISIN) {
    var dateInDate = DV.dateFactory(dateIn);

    if (DV.isValidDate(dateInDate)) {
        var sequence = DV.sequenceFactory(dateInDate, frequency);
    } else {
        sequence = "";
        error = true;
        errorMessage.push("NAV not updated as invalid date entered for ISIN");
    }
    return sequence;
}


