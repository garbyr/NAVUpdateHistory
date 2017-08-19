// dependencies
const aws = require('aws-sdk');
aws.config.update({ region: 'eu-west-1' });
var DV = require('./dateHelper.js');
//globals
error = false;
errorMessage = [];


exports.handler = (event, context, callback) => {
    //parse the event from SNS
    console.log(event);
    var dateSequence = new Date().getTime().toString();
    var dateTime = new Date().toUTCString();
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
        raiseError((input.ISIN, input.NAV, newSequence, dateSequence, input.requestUUID, dateTime, input.user, callback));
    }
    var expectedLastSequence = getExpectedLastSequence(newSequence, input.calculationDate);
    var sequenceFloor = parseInt(newSequence) - 500;
    console.log("INFO: sequence in " + newSequence);
    console.log("INFO: last sequence " + lastSequence);
    console.log("INFO: expected last sequence " + expectedLastSequence);
    console.log("INFO: sequence floor " + sequenceFloor);
    /*
     if (expectedLastSequence > LastSequence) {
         raiseError(ISIN, NAV, sequence, dateSequence, requestUUID, dateTime, user, "Incorrect sequence for frequency " + frequency +" : the expected sequence > actual last sequence");
         context.fail();
         return;
     } else if (lastSequence < sequenceFloor) {
         raiseError(ISIN, NAV, sequence, dateSequence, requestUUID, dateTime, user, "Incorrect sequence for frequency " + frequency +" : the sequence is outside the the ESMA calculation range");
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
            errorMessage.push("NAV History record update failed");
            raiseError((input.ISIN, input.NAV, newSequence, dateSequence, input.requestUUID, dateTime, input.user, callback));
        }
        else {
            console.log("SUCCESS: writing NAV History record", data);
            console.log("process SRRI = ", input.calculateSRRI);
            if (input.calculateSRRI == "Yes") {
                var response = {
                    requestUUID: input.requestUUID,
                    ISIN: input.ISIN,
                    NAV: input.NAV,
                    sequence: newSequence,
                    frequency: input.frequency,
                    category: input.category,
                    user: input.user,
                    shareClassDescription: input.description
                }
                var output={
                    status: "200",
                    response: response
                }
                console.log("requesting calculation preparation", response);
                callback(null, {response});          
            }
        }
    });
}

getLastSequence = function (context, input, callback, dateSequence, dateTime, _callback) {
    var lastSequence = 123;
        console.log("INFO: callback to main process");
       _callback(context, input, callback, dateSequence, dateTime, lastSequence);
}

getExpectedLastSequence = function (sequence, dateIn) {
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
        var date = new Date(Date.UTC(parseInt(dateIn.substr(6, 4)), (parseInt(dateIn.substr(3, 2) - 1)), parseInt(dateIn.substr(0, 2)), 0, 0, 0));
        var year = parseInt(date.getFullYear());
        var expectedYear = (year - 1).toString();
        var weeksInYear = new DV.getWeeksInYearForYear(expectedYear);
        expectedLastSequence = expectedYear + weeksInYear;

    }
        return expectedLastSequence;
 }

raiseError = function (ISIN, sequence, dateSequence, requestUUID, dateTime, user, callback) {
    //write to the database
     var errorObj = {
        requestUUID: requestUUID,
        ISIN:ISIN,
        sequence: sequence,
        user: user,
        messages: errorMessage,
    }
    NAVHistoryError.prototype = Error();
    const error = new NAVHistoryError(errorObj);
    callback(error);
}

createSequence = function (dateIn, frequency, ISIN) {
    var dateInDate = DV.dateFactory(dateIn);

    if (DV.isValidDate(dateInDate)) {
        var sequence = DV.sequenceFactory(dateInDate, frequency);
    } else {
        sequence = "";
        error = true;
        errorMessage.push("ISIN: " + ISIN + "- Invalid date entered, NAV not updated");
    }
    return sequence;
}


