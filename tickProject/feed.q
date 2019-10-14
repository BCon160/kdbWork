//// feed.q ////
//Author: Brendan Connolly
//Description: Mock feedhandler for the Advanced kdb+ exam.  Connects to the tp and publishes dummy records

//Usage:
/q feed.q [host]:port[:usr:pwd]

\l utilities.q

\d .u

simulate:{[t]
    numRecords:first 1?20;
    //Send half the number of trade records as quote records
    if[t = `trade;
        numRecords:numRecords div 2
    ];
    records:numRecords ?/: (1000000000;`VOD.L`BARC.L`AZN.L`BP.L`AV.L;100.0;10000);
    //Make sure that the time column is in ascending order
    records:@[records;0;asc];
    //Change from longs to times
    records:@[records;0;+;.z.n];
    //Quote table has two extra columns
    if[t = `quote;
        records,: numRecords ?/: (100.0;10000)
    ];
    records
 };

publish:{
    trade:simulate[`trade];
    quote:simulate[`quote];
    neg[tp](`.u.upd; `trade; trade);
    neg[tp](`.u.upd; `quote; quote);
 };

//Open handle to the tp
tp:hopen `$":",first .z.x,(count .z.x)_enlist(":5010")

\d .

//Publish records every second
.z.ts:{.u.publish[]}
system"t 1000"

//Load in the extra logging script if required
.utils.extraLogs[];

//Globals used
// .u.tp:handle to the tp
