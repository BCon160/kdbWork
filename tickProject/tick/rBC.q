//// r.q (edited for Advanced kdb+ exam) ////
//Author: Brendan Connolly
//Description: Normal r.q with extra features added in line with Advanced kdb+ CMTP.
//Note: Any code I write won't follow the same terse format as the normal r.q code!
//All double slashes below are edits that I have made

//Add an option to input the tables to subscribe to
//SUB_TABLES takes the form of a comma separated list of tables that the user wants to subscribe to
/q tick/rBC.q [host]:port[:usr:pwd] -SUB_TABLES subscriptions -HDB_CON [host]:port[:usr:pwd] -HDB_DIR dataDirectory

if[not "w"=first string .z.o;system "sleep 1"];

upd:insert;

//For html interface, set .z.ws to do something useful
.z.ws:{neg[.z.w].Q.s value x};

//Load in utility fuctions (getOpts)
\l utilities.q

//Set default to all if no tables are specified
.u.tabs:$[count tmp:.utils.getOpts["-SUB_TABLES"];
    `$"," vs tmp;
    `$()
 ];

/ end of day: save, clear, hdb reload
.u.end:{
    t:tables`.;
    t@:where `g=attr each t@\:`sym;
    //Use getOpts rather than positional argument for HDB details
    //Port default: 5014
    hdbConn:$[count tmp:.utils.getOpts["-HDB_CON"];
        tmp; 
        ":5014"
    ];
    .Q.hdpf[`$":",hdbConn;`:.;x;`sym];
    @[;`sym;`g#] each t;
 };

/ init schema and sync up from log file;cd to hdb(so client save can run)
.u.rep:{
    (.[;();:;].)each x;
    if[null first y;
        :()
    ];
    //For log replay only consider updates to tables we are subscried to
    upd::{[t;x] if[t in .u.tabs; t insert x]}; 
    -11!y;
    //Reset the upd function back to normal for all tp updates
    upd::{[t;x] t insert x}
 };

.u.init:{[tabs]
    //Get rid of the "," in front of a singleton list so it doesn't cause an error
    if[1 = count tabs;
        tabs:first tabs
     ];

    //Pass the correct parameters to .u.sub
    $[not count tabs;
        / connect to ticker plant for (schema;(logcount;log))
        tpInfo:(hopen `$":",.z.x 0)"(.u.sub[`;`];`.u `i`L)";
        tpInfo:(hopen `$":",.z.x 0)raze ("(.u.sub[";-1_.Q.s tabs;";`];`.u `i`L)")
     ];

    //Normalise tpInfo
    if[-11h = type first first tpInfo; tpInfo:@[tpInfo;0;enlist]];
    .[.u.rep;tpInfo];

    //Load in the extra logging script if required
    .utils.extraLogs[];

    //Get the hdb directory from command line
    hdbDir:$[count tmp:.utils.getOpts["-HDB_DIR"]; tmp; "db/"];
    //Change directory to user's choice of hdbdir
    system"cd ",hdbDir;
 };

.u.init[.u.tabs];

//Globals used
// .u.tabs - List of tables that the rdb will subscribe to
