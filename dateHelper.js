
  getWeek = function(date) {
  date.setHours(0, 0, 0, 0);
  // Thursday in current week decides the year.
  date.setDate(date.getDate() + 3 - (date.getDay() + 6) % 7);
  // January 4 is always in week 1.
  var week1 = new Date(date.getFullYear(), 0, 4);
  // Adjust to Thursday in week 1 and count number of weeks from date to week1.
  return 1 + Math.round(((date.getTime() - week1.getTime()) / 86400000
                        - 3 + (week1.getDay() + 6) % 7) / 7);
}

// Returns the four-digit year corresponding to the ISO week of the date.
getWeekYear = function(date) {
  date.setDate(date.getDate() + 3 - (date.getDay() + 6) % 7);
  return date.getFullYear();
}

getWeeksInYear = function(date){
  var year = date.getFullYear();
  var lastDay = ('12/31/'+year).toString();
  var lastDayDate = new Date(lastDay);
  var weeksInYear = Date.prototype.getWeek(lastDayDate);
  return weeksInYear;
}

formatForJS= function(dateStringIn){
  var dateStringOut = dateStringIn.replace(/^(\d{1,2}\/)(\d{1,2}\/)(\d{4})$/, "$2$1$3");
  return dateStringOut;
}

dateFactory=function(dateIn){
    var dateArray = dateIn.split('/');
    var year = (parseInt(dateArray[2]));
    if(year < 1900){
      return null;
    }
    var month = parseInt(dateArray[1]-1);
    if((month < 0) || (month > 11)){
      return null;
    }
    var day =  parseInt(dateArray[0]);
    if(day < 1){
      return null;
    } else if((month ==8)|| (month==3) || (month == 5) || (month==10)){
      if(day > 30 ){
        return null;
      }
    }else if(month == 2){
      if(leapYear(year)){
        if(day > 29){
          return null;
        }
      } else {
        if(day > 28){
          return null;
        }
      }
      //not a leap year
    } else{
      if(day > 31){
        return null;
      }
    }
   
    var dateInDate = new Date(Date.UTC(year, month,day,0,0,0));
    return dateInDate;
}

isValidDate=function(date){
   return (date != null);
}

sequenceFactory=function(dateInDate, frequency){
     //check if valid date, if not error
    var sequence;
    if(frequency=="Weekly"){
    //get the year & week
    var year = getWeekYear(dateInDate).toString();
    var week = getWeek(dateInDate).toString();
    //if week < 10 substring add leading 0
    if(week.length==1){
        week = "0" + week;
    }
    sequence = year + week;
    }
    return sequence;
}

getWeeksInYearForYear = function(year){
  var lastDay = ('12/31/'+year).toString();
  var lastDayDate = new Date(lastDay);
  var weeksInYear = Date.prototype.getWeek(lastDayDate);
  return weeksInYear;
}

function leapYear(year)
{
  return ((year % 4 == 0) && (year % 100 != 0)) || (year % 400 == 0);
}

exports.formatForJS = formatForJS;
exports.getWeekYear = getWeekYear;
exports.getWeek = getWeek;
exports.getWeeksInYear = getWeeksInYear;
exports.sequenceFactory = sequenceFactory;
exports.getWeeksInYearForYear = getWeeksInYearForYear;
exports.dateFactory = dateFactory;
exports.isValidDate = isValidDate;



