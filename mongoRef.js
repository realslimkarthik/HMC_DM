// Commonly used MongoDB queries:

// Query for getting data after a date:
db.coll_name.find({'pT': {$gte: new Date('Sep 1, 2014')}})

// Query for checking if a value exists in an array:
db.coll_name.find({'mrv': {'$in': ['22']}})

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