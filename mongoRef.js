// Commonly used MongoDB queries:

mongo -h 10.177.54.27/twitter -u HMCUser -authenticationDatabase admin -p Reader

// Query for getting data after a date:
db.coll_name.find({'pT': {$gte: new Date('Sep 1, 2014')}})

// Query for finding all tweets that have a rule 22 and/or 23 (from:uwctri, from:venomocity):
db.coll_name.find({'mrv': {'$in': [22, 23]}})

// Query for finding all tweets that have a tag 7 (Ecig):
db.coll_name.find({'mrt': {'$in': [7]}})
// fields

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

// Function to initialize all the collection names. Add more as and when needed.
function initMonths() {
    Jan14 = ['Jan14_1', 'Jan14_2', 'Jan14_3', 'Jan14_4', 'Jan14_5', 'Jan14_6'];
    Feb14 = ['Feb14_1', 'Feb14_2', 'Feb14_3', 'Feb14_4', 'Feb14_5', 'Feb14_6'];
    Mar14 = ['Mar14_1', 'Mar14_2', 'Mar14_3', 'Mar14_4', 'Mar14_5', 'Mar14_6'];
    Apr14 = ['Apr14_1', 'Apr14_2', 'Apr14_3', 'Apr14_4', 'Apr14_5', 'Apr14_6'];
    May14 = ['May14_1', 'May14_2', 'May14_3', 'May14_4', 'May14_5', 'May14_6'];
    Jun14 = ['Jun14_1', 'Jun14_2', 'Jun14_3', 'Jun14_4', 'Jun14_5', 'Jun14_6'];
    Jul14 = ['Jul14_1', 'Jul14_2', 'Jul14_3', 'Jul14_4', 'Jul14_5', 'Jul14_6'];
    Aug14 = ['Aug14_1', 'Aug14_2', 'Aug14_3', 'Aug14_4', 'Aug14_5', 'Aug14_6'];
    Sep14 = ['Sep14_1', 'Sep14_2', 'Sep14_3', 'Sep14_4', 'Sep14_5', 'Sep14_6'];
    Oct14 = ['Oct14_1', 'Oct14_2', 'Oct14_3', 'Oct14_4', 'Oct14_5', 'Oct14_6'];
    Nov14 = ['Nov14_1', 'Nov14_2', 'Nov14_3', 'Nov14_4', 'Nov14_5', 'Nov14_6'];
    Dec14 = ['Dec14_1', 'Dec14_2', 'Dec14_3', 'Dec14_4', 'Dec14_5', 'Dec14_6'];
    Jan15 = ['Jan15_1', 'Jan15_2', 'Jan15_3', 'Jan15_4', 'Jan15_5', 'Jan15_6'];
    Feb15 = ['Feb15_1', 'Feb15_2', 'Feb15_3', 'Feb15_4', 'Feb15_5', 'Feb15_6'];
    Mar15 = ['Mar15_1', 'Mar15_2', 'Mar15_3', 'Mar15_4', 'Mar15_5', 'Mar15_6'];
    Apr15 = ['Apr15_1', 'Apr15_2', 'Apr15_3', 'Apr15_4', 'Apr15_5', 'Apr15_6'];
    May15 = ['May15_1', 'May15_2', 'May15_3', 'May15_4', 'May15_5', 'May15_6'];
}

// Function to initalize a JS Object which has the collection names under each month's key
function initYear(year) {
    y = {};
    yStr = year.toString();
    y['Jan' + yStr] = ['Jan'+ yStr + '_1', 'Jan'+ yStr + '_2', 'Jan'+ yStr + '_3', 'Jan'+ yStr + '_4', 'Jan'+ yStr + '_5', 'Jan'+ yStr + '_6'];
    y['Feb' + yStr] = ['Feb'+ yStr + '_1', 'Feb'+ yStr + '_2', 'Feb'+ yStr + '_3', 'Feb'+ yStr + '_4', 'Feb'+ yStr + '_5', 'Feb'+ yStr + '_6'];
    y['Mar' + yStr] = ['Mar'+ yStr + '_1', 'Mar'+ yStr + '_2', 'Mar'+ yStr + '_3', 'Mar'+ yStr + '_4', 'Mar'+ yStr + '_5', 'Mar'+ yStr + '_6'];
    y['Apr' + yStr] = ['Apr'+ yStr + '_1', 'Apr'+ yStr + '_2', 'Apr'+ yStr + '_3', 'Apr'+ yStr + '_4', 'Apr'+ yStr + '_5', 'Apr'+ yStr + '_6'];
    y['May' + yStr] = ['May'+ yStr + '_1', 'May'+ yStr + '_2', 'May'+ yStr + '_3', 'May'+ yStr + '_4', 'May'+ yStr + '_5', 'May'+ yStr + '_6'];
    y['Jun' + yStr] = ['Jun'+ yStr + '_1', 'Jun'+ yStr + '_2', 'Jun'+ yStr + '_3', 'Jun'+ yStr + '_4', 'Jun'+ yStr + '_5', 'Jun'+ yStr + '_6'];
    y['Jul' + yStr] = ['Jul'+ yStr + '_1', 'Jul'+ yStr + '_2', 'Jul'+ yStr + '_3', 'Jul'+ yStr + '_4', 'Jul'+ yStr + '_5', 'Jul'+ yStr + '_6'];
    y['Aug' + yStr] = ['Aug'+ yStr + '_1', 'Aug'+ yStr + '_2', 'Aug'+ yStr + '_3', 'Aug'+ yStr + '_4', 'Aug'+ yStr + '_5', 'Aug'+ yStr + '_6'];
    y['Sep' + yStr] = ['Sep'+ yStr + '_1', 'Sep'+ yStr + '_2', 'Sep'+ yStr + '_3', 'Sep'+ yStr + '_4', 'Sep'+ yStr + '_5', 'Sep'+ yStr + '_6'];
    y['Oct' + yStr] = ['Oct'+ yStr + '_1', 'Oct'+ yStr + '_2', 'Oct'+ yStr + '_3', 'Oct'+ yStr + '_4', 'Oct'+ yStr + '_5', 'Oct'+ yStr + '_6'];
    y['Nov' + yStr] = ['Nov'+ yStr + '_1', 'Nov'+ yStr + '_2', 'Nov'+ yStr + '_3', 'Nov'+ yStr + '_4', 'Nov'+ yStr + '_5', 'Nov'+ yStr + '_6'];
    y['Dec' + yStr] = ['Dec'+ yStr + '_1', 'Dec'+ yStr + '_2', 'Dec'+ yStr + '_3', 'Dec'+ yStr + '_4', 'Dec'+ yStr + '_5', 'Dec'+ yStr + '_6'];
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

// Query to change mrv values into ints from strings:
db.coll_name.find().forEach(function(doc) {
    id = doc._id;
    a = [];
    doc.mrv.forEach(function(e) {
        a.push(parseInt(e));
    });
    db.coll_name.update(
        {"_id": id},
        {"$set": {"mrv": a}
    });
    print(id);
    print(a);
})

// Query to delete records entered into the wrong collections:
db.coll_name.find().forEach(function(doc) {
    id = doc._id;
    db.coll_name.remove({"_id": id});
    print(id);
})

// Query to change pt values into ISODates from strings:
db.coll_name.find().forEach(function(doc) {
    var id = doc._id;
    var d;
    if(typeof doc.pt == "string") {
        d = doc.pt.split('T');
        time = d[1].split(':');
        dateObj = Date.parse(d[0] + ' ' + time[0] + ':' + time[1] + ':' time[2]);
        print(id);
        print(d);
    }
})

// Query to convert date strings into ISO dates
db.coll_name.find().forEach(function(doc) {
    db.coll_name.update({'_id':doc._id},{'$set':{'pt':new Date(doc.pt)}});
})

// Query to convert hashtags field to a list instead of a ';' delimited string
db.coll_name.find().forEach(function(doc) {
    if (typeof doc['eumh'] == 'string') {
        print('changed!');
        db.coll_name.update({'_id': doc._id}, {'$set': {'eumh': doc['eumh'].split(';')}});
    }
})

// Data Structures and Code to index Tags
tags = {"antismoking": 1, "cdc tips ii": 2, "cessation product": 3, "chew": 4,
        "cigar/cigarillo": 5, "cigarette": 6, "ecig": 7, "general": 8,
        "hookah": 9, "marijuana": 10, "pipe": 11, "state public health": 12,
        "legacy campaign": 13, "goldilocks": 14
};

function indexTags(coll_name) {
    db[coll_name].ensureIndex({'mrtI': 1});
    db[coll_name].find().forEach(function(doc) {
        t = doc['mrt'].split(';');
        newTags = [];
        t.forEach(function(e) {
            element = e.toLowerCase().trim();
            tag = tags[element];
            if(tag == undefined)
                {print("undefined" + element);}
            else {
                if (element == "legacy caompaign") {element = "legacy campaign";}
                if (newTags.indexOf(tag) == -1)
                    {newTags.push(tag);}
            }
        });
        db[coll_name].update({'_id': doc._id}, {'$set': {'mrtI': newTags}});
        print(newTags);
    });
}

function indexTags(coll_name) {
    db[coll_name].find().forEach(function(doc) {
        print(coll_name + doc.mrt.toString());
        db[coll_name].update({'_id': doc._id}, {'$set':{'mrt': doc.mrtI}});
    });
}

function getLocationFraction(gnip) {
    yearCount = 0;
    totalYear = 0;
    year.forEach(function(m) {
        monthlyCount = 0;
        totalMonth = 0;
        m.forEach(function(c) {
            totalMonth = db[c].count();
            if (gnip == true) {
                monthlyCount += db[c].count({'gplg': {$exists: true}});
            } else {
                monthlyCount += db[c].count({'': {$exists: true}});
            }
        });
        yearCount += monthlyCount;
        totalYear += totalMonth;
        if (gnip) {
            print('for the month of ' + m + ': ' + monthlyCount/totalMonth + '% of tweets have gnip location data');
        } else {
            print('for the month of ' + m + ': ' + monthlyCount/totalMonth + '% of tweets have location data');
        }
    });
    if (gnip == true) {
        print('The year of 2014 has ' + yearCount/totalYear + '% of tweets with gnip location data');
    } else {
        print('The year of 2014 has ' + yearCount/totalYear + '% of tweets with location data');
    }
}