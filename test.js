// Our Lambda function fle is required 
var index = require('./NAVUpdateHistory.js');

// The Lambda context "done" function is called when complete with/without error
var context = {
    done: function (err, result) {
        console.log('------------');
        console.log('Context done');
        console.log('   error:', err);
        console.log('   result:', result);
    }
};

// Simulated S3 bucket event
var event = {
                    calculateSRRI: 'Yes',
                    requestUUID: 'abc123',
                    ISIN: 'x12345',
                    NAV: "0.123",
                    frequency: 'Weekly',
                    category: 'Market',
                    user: 'Gary',
                    calculationDate: '21/07/2017',
                    description: 'ICIN x12345'
                }
        
// Call the Lambda function
index.handler(event, context);