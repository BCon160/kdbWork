//Utilities.q
//Author: Brendan Connolly
//Description: This script contains tasks 7-9 from Advanced kdb+ CMTP

//Usage: 
//  q dataHelpers.q -tpPort 5010 -tpLog tpLog
//Note: This script requires the tp to be running to function

0N!"Call .logFilt.usage for instructions on how to use the log filtering tool";
0N!"Call .csvLoad.usage for instructions on how to use the csv publishing tool";
0N!"Call .eod.usage for instructions on how to use the log eod tool";

//Load in utility functions (getOpts etc..)
\l ./utilities.q

//Get handle to tp (All parts below will need this)
.cfg.tp:.utils.getOpts["-tpPort"];
.cfg.tp:hopen `$":",$[count .cfg.tp; ":",.cfg.tp; ":5010"];
//Get location of tp log dir from command line, default is ./tpLog
.cfg.tpLogLoc:$[count tmp:.utils.getOpts["-tpLog"];`$":", tmp; `:tpLog];
//Get schemas dictionary (tableName -> schema)
.cfg.schemas:.cfg.tp"tables[]!0#/: value each tables[]";

////////////// Filter Logs ////////////////
//Produces a log with all syms filtered out but a single sym
//Note: I had written another version of this that manually went though each item in a log list, filtered and re-saved but decided that -11! was going to be much faster.  The two globals introduced are a reasonable price to pay
//This code assumes an uncorrupted log
//Parameters
//  logName - Name of tp log file
//  filterSym - Instrument to be filtered on
\d .logFilt
filterLog:{[logName;filtSym]
    //Get paths to unfiltered and filtered logs
	rawLog:` sv (.cfg.tpLogLoc;logName);
    newLog::` sv (.cfg.tpLogLoc;.Q.dd[logName;filtSym]);

    //Initialise the new log as an empty list
    newLog set ();

    //Globalise the filter condition to be used in the 
    filterSym::filtSym;
    
    `upd set {[t;x]
        colIdx:(cols[.cfg.schemas t])?`sym;
        idxs:where x[colIdx] = .logFilt.filterSym;
        if[count x:x[;idxs];
            .logFilt.newLog upsert enlist(`upd; t; x)
        ];
    };

	-11!rawLog;
 };

usage:{
    0N!"Usage: .logFilt.filterLog[logName; filterSym]";
    0N!"Args:   logName<kdbFilePath> -> path and name of tp log file that is to be filtered";
    0N!"        filterSym<symbol> -> Instrument that the new log is to be filtered by";
 };
\d .
//Globals used
//  .logFilt.newLog - Path to new filtered log file
//  .logfilt.filterSym - Instrument to filter the new log on (has to be global as it is needed inside the upd func and can't be passed in
///////////////////////////////////////////

/////////////// CSV Load //////////////////
\d .csvLoad

csvToTP:{[dir;fileName;t;head]
    path:` sv (dir;fileName);
    firstRun::$[head; 1b; 0b];
    .Q.fs[readAndPubCSVchunk[;enlist[t]#.cfg.schemas];path];
 };

readAndPubCSVchunk:{[data;schema]
    //Make sure to skip the header line on the first chunk
    if[firstRun;
        data:1_data;
        firstRun::0b
    ];
    //For the sake of this excersize I will assume only CSVs
    //Also assuming that there are no nested columns apart from strings
    typs:ssr[upper exec t from meta first value schema;" "; "*"];
    data:(typs; ",") 0: data;
    pub[first key schema;data];
 };

pub:{[t;x]
    neg[.cfg.tp] (`.u.upd; t; x);
 };

usage:{
    0N!"Usage: .csvLoad.csvToTP[directory; fileName; tableName; head]";
    0N!"Args:   directory<kdbFilePath> -> Directory in which the csv resides";
    0N!"        fileName<symbol> -> name of csv file";
    0N!"        tableName<symbol> -> Name of table contained in csv file, must match name in schema file";
    0N!"        head<boolean> -> If the csv file contains a header row, set this flag to true";
 };
\d .
///////////////////////////////////////////

/////////////// EOD proc //////////////////
\d .eod
//Note this script should always be run from the directory above the database directory

//Get the exact Logfile to replay, set the directory to replay to and initialise the tables on disk
init:{[logName;dt]
    //Dir: database directory
    if[not count @[get; `.eod.dir; ()];
	    dir::`:db;
    ];

    //dir+date
    path::` sv (dir;`$string dt);

    //Set empty tables on disk
	{(` sv (path; x; `)) set .Q.en[dir;y]}'[key .cfg.schemas; value .cfg.schemas];

    //Logfile path
	` sv (.cfg.tpLogLoc;logName)
 };

//Provide a way for the user to set the database directory
setDir:{[newDir]
	dir::newDir;
 };
 
compress:{[columnPath]
    -19!(columnPath;columnPath;17;2;6)
 };

run:{[logName;dt]
    //Initialise globals for this run and get logfile path
	logFile:init[logName;dt];
    
    //Set the upd function for this log replay
    `upd set {[t;x]
	    x:flip cols[.cfg.schemas t]!x;
	    (` sv (.eod.path; t; `)) upsert .Q.en[.eod.dir;x]
    };

    //Replay the log file
	-11!logFile;
    
    
    allColPaths:` sv/: path,/: raze key[.cfg.schemas],/:'cols each value .cfg.schemas;
    //Need to compress data
    compress each allColPaths;
 };

usage:{
    0N!"Usage: .eod.run[logName;date]";
    0N!"Args:   logName<symbol> -> Name of log file to be replayed to disk, log location can be set from the command line"; 
    0N!"        date<date> -> Date partition to write to";
 };
\d .
//Globals used:
//  .eod.dir - Directory that the data will be saved to
//  .eod.path - ./<.eod.dir>/<date>
///////////////////////////////////////////
