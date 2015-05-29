// Commonly used MongoDB queries:

mongo -h 10.177.54.27/twitter -u HMCUser -authenticationDatabase admin -p Reader

// Query for getting data after a date:
db.coll_name.find({'pT': {$gte: new Date('Sep 1, 2014')}})

// Query for finding all tweets that have a rule 22 and/or 23 (from:uwctri, from:venomocity):
db.coll_name.find({'mrv': {'$in': [22, 23]}})

// Query for finding all tweets that have a tag 7 (Ecig):
db.coll_name.find({'mrt': {'$in': [7]}})


// Twitter Fields
// _id     =   Idpost
// bp      =   bodypost
// rc      =   retweetCount
// gd      =   generatordname
// fl      =   filterlevel
// ks      =   kloutscore
// mrt     =   matchingrulestag
// mrv     =   matchingrulesvalue
// lv      =   languagevalue
// fc      =   favoritesCount
// opt     =   objpostedTime
// os      =   objsummary
// ol      =   objlink
// oi      =   objid
// oot     =   objobjType
// bo      =   bodyoriginal
// apu     =   actorpreferredusername
// an      =   actorname
// alh     =   actorlinkshref
// atz     =   actorTimeZone
// ai      =   actorimage
// av      =   actorverified
// asc     =   actorstatusuesCount
// as      =   actorsummary
// al      =   actorlanguages
// auO     =   actorutcOffset
// alk     =   actorlink
// afc     =   actorfollowersCount
// afa     =   actorfavoritesCount
// afr     =   actorfriendsCount
// alc     =   actorlistedCount
// apt     =   actorPostedTime
// aid     =   actorid
// aot     =   actorObjType
// eum     =   entitiesusrmentions
// eumh    =   entitieshtagstext
// euu     =   entitiesurlexpandedurl
// emu     =   entitiesmdaexpandedurl
// l       =   lang
// pt      =   postedTime
// ogt     =   objgeotype
// old     =   objlocdname
// gt      =   geotype
// gc      =   geocoordinates
// v       =   verb
// lk      =   link
// ld      =   locdname
// ln      =   locname
// ltc     =   loctwcountrycode
// lcc     =   loccountrycode
// lgc     =   locgeocoordinates
// gpld    =   gniplocdname
// gplco   =   gniploccountry
// gplr    =   gniplocregion
// gplc    =   gniploccounty
// gpll    =   gniploclocality
// gplg    =   gniplocgeocoordinates
// gu      =   gnipurl
// geu     =   gnipexpandedurl

// Instagram Fields
// _id     =   id
// u       =   updated
// p       =   published
// t       =   title
// la      =   linkalternate
// ls      =   linkself
// le      =   linkenclosure
// lp      =   linkpreview
// su      =   sourceupdated
// aot     =   actobjactobjtype
// aoc     =   actobjcontent
// aogp    =   actobjgeopoint
// aoct    =   actobjcategoryterm
// aogs    =   actobjfavcount
// an      =   authname
// au      =   authuri
// mrv     =   matchingrulesvalue
// mrt     =   matchingrulestag


// Function to initialize all the collection names. Add more as and when needed.
function initMonths() {
    Jan14 = ['201401_1', '201401_2', '201401_3', '201401_4', '201401_5', '201401_6'];
    Feb14 = ['201402_1', '201402_2', '201402_3', '201402_4', '201402_5', '201402_6'];
    Mar14 = ['201403_1', '201403_2', '201403_3', '201403_4', '201403_5', '201403_6'];
    Apr14 = ['201404_1', '201404_2', '201404_3', '201404_4', '201404_5', '201404_6'];
    May14 = ['201405_1', '201405_2', '201405_3', '201405_4', '201405_5', '201405_6'];
    Jun14 = ['201406_1', '201406_2', '201406_3', '201406_4', '201406_5', '201406_6'];
    Jul14 = ['201407_1', '201407_2', '201407_3', '201407_4', '201407_5', '201407_6'];
    Aug14 = ['201408_1', '201408_2', '201408_3', '201408_4', '201408_5', '201408_6'];
    Sep14 = ['201409_1', '201409_2', '201409_3', '201409_4', '201409_5', '201409_6'];
    Oct14 = ['201410_1', '201410_2', '201410_3', '201410_4', '201410_5', '201410_6'];
    Nov14 = ['201411_1', '201411_2', '201411_3', '201411_4', '201411_5', '201411_6'];
    Dec14 = ['201412_1', '201412_2', '201412_3', '201412_4', '201412_5', '201412_6'];
    Jan15 = ['201501_1', '201501_2', '201501_3', '201501_4', '201501_5', '201501_6'];
    Feb15 = ['201502_1', '201502_2', '201502_3', '201502_4', '201502_5', '201502_6'];
    Mar15 = ['201503_1', '201503_2', '201503_3', '201503_4', '201503_5', '201503_6'];
    Apr15 = ['201504_1', '201504_2', '201504_3', '201504_4', '201504_5', '201504_6'];
    May15 = ['201505_1', '201505_2', '201505_3', '201505_4', '201505_5', '201505_6'];
}

// Function to initalize a JS Object which has the collection names under each month's key
function initYear(year) {
    y = {};
    yStr = year.toString();
    y['Jan' + yStr] = [yStr + '01' + '_1', yStr + '01' + '_2', yStr + '01' + '_3', yStr + '01' + '_4', yStr + '01' + '_5', yStr + '01' + '_6'];
    y['Feb' + yStr] = [yStr + '02' + '_1', yStr + '02' + '_2', yStr + '02' + '_3', yStr + '02' + '_4', yStr + '02' + '_5', yStr + '02' + '_6'];
    y['Mar' + yStr] = [yStr + '03' + '_1', yStr + '03' + '_2', yStr + '03' + '_3', yStr + '03' + '_4', yStr + '03' + '_5', yStr + '03' + '_6'];
    y['Apr' + yStr] = [yStr + '04' + '_1', yStr + '04' + '_2', yStr + '04' + '_3', yStr + '04' + '_4', yStr + '04' + '_5', yStr + '04' + '_6'];
    y['May' + yStr] = [yStr + '05' + '_1', yStr + '05' + '_2', yStr + '05' + '_3', yStr + '05' + '_4', yStr + '05' + '_5', yStr + '05' + '_6'];
    y['Jun' + yStr] = [yStr + '06' + '_1', yStr + '06' + '_2', yStr + '06' + '_3', yStr + '06' + '_4', yStr + '06' + '_5', yStr + '06' + '_6'];
    y['Jul' + yStr] = [yStr + '07' + '_1', yStr + '07' + '_2', yStr + '07' + '_3', yStr + '07' + '_4', yStr + '07' + '_5', yStr + '07' + '_6'];
    y['Aug' + yStr] = [yStr + '08' + '_1', yStr + '08' + '_2', yStr + '08' + '_3', yStr + '08' + '_4', yStr + '08' + '_5', yStr + '08' + '_6'];
    y['Sep' + yStr] = [yStr + '09' + '_1', yStr + '09' + '_2', yStr + '09' + '_3', yStr + '09' + '_4', yStr + '09' + '_5', yStr + '09' + '_6'];
    y['Oct' + yStr] = [yStr + '10' + '_1', yStr + '10' + '_2', yStr + '10' + '_3', yStr + '10' + '_4', yStr + '10' + '_5', yStr + '10' + '_6'];
    y['Nov' + yStr] = [yStr + '11' + '_1', yStr + '11' + '_2', yStr + '11' + '_3', yStr + '11' + '_4', yStr + '11' + '_5', yStr + '11' + '_6'];
    y['Dec' + yStr] = [yStr + '12' + '_1', yStr + '12' + '_2', yStr + '12' + '_3', yStr + '12' + '_4', yStr + '12' + '_5', yStr + '12' + '_6'];
    return y;
}

// Function to count or find tweets in a month
function monthlyCounts(coll_names, ruleOrTag, rules) {
    var total = 0;
    var count = 0;
    var successful;
    coll_names.every(function(c) {
        successful = true;
        if (ruleOrTag === "rule") {
            count = db[c].count({'mrv':{$in:rules}});
        } else if (ruleOrTag === "tag") {
            count = db[c].count({'mrt':{$in:rules}});
        } else {
            print("Please enter \"rule\" or \"tag\" as the second parameter");
            successful = false;
            return false;
        }
        print(c + ":");
        total += count;
        print(count);
        print("\n");
        return successful;
    });
    if (successful) {
        print("Total count: " + total);
    }
}













// =================================================================== DANGER ========================================================
// // Query to change mrv values into ints from strings:
// db.coll_name.find().forEach(function(doc) {
//     id = doc._id;
//     a = [];
//     doc.mrv.forEach(function(e) {
//         a.push(parseInt(e));
//     });
//     db.coll_name.update(
//         {"_id": id},
//         {"$set": {"mrv": a}
//     });
//     print(id);
//     print(a);
// })

// // Query to delete records entered into the wrong collections:
// db.coll_name.find().forEach(function(doc) {
//     id = doc._id;
//     db.coll_name.remove({"_id": id});
//     print(id);
// })

// // Query to change su, u and p values into ISODates from strings (Instagram):
// db.getCollection('201408').find().forEach(function(doc) {     
//     id = doc['_id'];
//     suObj = doc['su'];
//     uObj = doc['u'];
//     pObj = doc['p'];
//     print(id);
//     if(typeof doc['su'] == 'string') {
//         d = doc['su'].split('T');         
//         time = d[1].split('-');
//         if (time.length != 3) {
//             time = d[1].split(':');
//         }
//         suObj = new Date(d[0] + ' ' + time[0] + ':' + time[1] + ':' + time[2].slice(0, -1));
//     }
//     if(typeof doc['u'] == 'string') {
//         d = doc['u'].split('T');         
//         time = d[1].split(':');
//         if (time.length != 3) {
//             time = d[1].split('-');
//         }
//         uObj = new Date(d[0] + ' ' + time[0] + ':' + time[1] + ':' + time[2].slice(0, -1));
//     }
//     if(typeof doc['p'] == 'string') {
//         d = doc['p'].split('T');         
//         time = d[1].split(':');
//         if (time.length != 3) {
//             time = d[1].split('-');
//         }
//         pObj = new Date(d[0] + ' ' + time[0] + ':' + time[1] + ':' + time[2].slice(0, -1));
//     }
//     db.getCollection('201408').update({'_id':id},
//         {'$set':{'su': suObj, 'u': uObj, 'p': pObj}});
// })

// // Query to convert date strings into ISO dates
// db.coll_name.find().forEach(function(doc) {
//     db.coll_name.update({'_id':doc._id}, {'$set':{'su':new Date(doc.su)}});
// })


// // Query to change pt values into ISODates from strings:
// db.coll_name.find().forEach(function(doc) {
//     var id = doc._id;
//     var d;
//     if(typeof doc.pt == "string") {
//         d = doc.pt.split('T');
//         time = d[1].split(':');
//         dateObj = Date.parse(d[0] + ' ' + time[0] + ':' + time[1] + ':' time[2]);
//         print(id);
//         print(d);
//     }
// })

// // Query to convert date strings into ISO dates
// db.coll_name.find().forEach(function(doc) {
//     db.coll_name.update({'_id':doc._id},{'$set':{'pt':new Date(doc.pt)}});
// })

// // Query to convert hashtags field to a list instead of a ';' delimited string
// db.coll_name.find().forEach(function(doc) {
//     if (typeof doc['eumh'] == 'string') {
//         print('changed!');
//         db.coll_name.update({'_id': doc._id}, {'$set': {'eumh': doc['eumh'].split(';')}});
//     }
// })

// // Data Structures and Code to index Tags
// tags = {"antismoking": 1, "cdc tips ii": 2, "cessation product": 3, "chew": 4,
//         "cigar/cigarillo": 5, "cigarette": 6, "ecig": 7, "general": 8,
//         "hookah": 9, "marijuana": 10, "pipe": 11, "state public health": 12,
//         "legacy campaign": 13, "goldilocks": 14
// };

// function indexTags(coll_name) {
//     db[coll_name].ensureIndex({'mrtI': 1});
//     db[coll_name].find().forEach(function(doc) {
//         t = doc['mrt'].split(';');
//         newTags = [];
//         t.forEach(function(e) {
//             element = e.toLowerCase().trim();
//             tag = tags[element];
//             if(tag == undefined)
//                 {print("undefined" + element);}
//             else {
//                 if (element == "legacy caompaign") {element = "legacy campaign";}
//                 if (newTags.indexOf(tag) == -1)
//                     {newTags.push(tag);}
//             }
//         });
//         db[coll_name].update({'_id': doc._id}, {'$set': {'mrtI': newTags}});
//         print(newTags);
//     });
// }

// function indexTags(coll_name) {
//     db[coll_name].find().forEach(function(doc) {
//         print(coll_name + doc.mrt.toString());
//         db[coll_name].update({'_id': doc._id}, {'$set':{'mrt': doc.mrtI}});
//     });
// }

// function getLocationFraction(gnip) {
//     yearCount = 0;
//     totalYear = 0;
//     year.forEach(function(m) {
//         monthlyCount = 0;
//         totalMonth = 0;
//         m.forEach(function(c) {
//             totalMonth = db[c].count();
//             if (gnip == true) {
//                 monthlyCount += db[c].count({'gplg': {$exists: true}});
//             } else {
//                 monthlyCount += db[c].count({'': {$exists: true}});
//             }
//         });
//         yearCount += monthlyCount;
//         totalYear += totalMonth;
//         if (gnip) {
//             print('for the month of ' + m + ': ' + monthlyCount/totalMonth + '% of tweets have gnip location data');
//         } else {
//             print('for the month of ' + m + ': ' + monthlyCount/totalMonth + '% of tweets have location data');
//         }
//     });
//     if (gnip == true) {
//         print('The year of 2014 has ' + yearCount/totalYear + '% of tweets with gnip location data');
//     } else {
//         print('The year of 2014 has ' + yearCount/totalYear + '% of tweets with location data');
//     }
// }