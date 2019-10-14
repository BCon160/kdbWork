//// tick.q (edited for Advanced kdb+ exam) ////
//Author: Brendan Connolly
//Description: Normal tick.q with extra features added such as logging in line with Advanced kdb+ CMTP.
//Note: Any code I write won't follow the same terse format as the normal tick.q code!
//All double slashed comments below are edits that I have made (excluding the formatting changes I have made)
 
/ q tickBC.q sym -tpLog . -NAME tp -p 5001 </dev/null >foo 2>&1 &

//Add various options
/q tickBC.q SRC -tpLog DST -loggingDir directory -NAME procName [-p 5010]
system"l tick/",(src:first .z.x,enlist"sym"),".q";

if[not system"p";system"p 5010"]

//Load in utility functions (.util.getOpts)
\l utilities.q
//Load in edited u.q script
\l tick/uBC.q
\d .u

ld:{
    if[not type key L::`$(-10_string L),string x;
        .[L;();:;()]
    ];
    i::j::-11!(-2;L);
    if[0<=type i;
        -2 (string L)," is a corrupt log. Truncate to length ",(string last i)," and restart";
        exit 1
    ];
    hopen L
 };

//Process log file init
loggingInit:{[dir; name]
    //Initialise last log time
    lastLogTime::00:00:00.000;
    //Open and return a handle to the loggingFile
    hopen hsym `$ raze (dir; $[not count name; "tp"; name]; "_"; ssr[string .z.p;":";"."])
 }

tick:{[schema; tpLogDir; loggingDir; procName]
    init[];
    if[not min(`time`sym~2#key flip value@)each t;
        '`timesym
    ];
    @[;`sym;`g#]each t;
    d::.z.D;
    if[l::count tpLogDir;
        L::`$":",tpLogDir,"/",schema,10#".";
        l::ld d
    ];
    //Add call to process log file init
    logH::loggingInit[loggingDir; procName]
 };

endofday:{
    end d;
    d+:1;
    if[l;
        hclose l;
        l::0(`.u.ld;d)
    ]
 };

ts:{
    //Add a call to the logging function
    logging[.z.p];
    if[d<x;
        if[d<x-1;
            system"t 0";
            '"more than one day?"
        ];
        endofday[]
    ]
 };

if[system"t";
    .z.ts:{
        pub'[t;value each t];
        @[`.;t;@[;`sym;`g#]0#];
        i::j;
        ts .z.D
     };
    upd:{[t;x]
        if[not -16=type first first x;
            if[d<"d"$a:.z.P;
                .z.ts[]
            ];
            a:"n"$a;
            x:$[0>type first x;
                a,x;
                (enlist(count first x)#a),x
            ]
        ];
        tCount[t]+:count first x;
        t insert x;
        if[l;
            l enlist (`upd;t;x);
            j+:1
        ];
     }
 ];

if[not system"t";
    system"t 1000";
    .z.ts:{ts .z.D};
    upd:{[t;x]
        ts"d"$a:.z.P;
        if[not -16=type first first x;
            a:"n"$a;
            x:$[0>type first x;
                a,x;
                (enlist(count first x)#a),x
            ]
        ];
        f:key flip value t;
        tCount[t]+:count first x;
        pub[t;$[0>type first x;enlist f!x;flip f!x]];
        if[l;
            l enlist (`upd;t;x);
            i+:1
        ];
     }
 ];

//Logging function
logging:{[timeNow]
    $[timeNow >= lastLogTime + `second$60;
        lastLogTime::timeNow;
        :(::)
    ];
    //Send number of updates to tables to log file
    logH raze(string .z.h; " "; string timeNow; " Table Update Numbers: \n");
    logH .Q.s[tCount], "\n";
    //Send subscription details to log file
    logH raze(string .z.h; " "; string timeNow; " Subscription Details: \n");
    logH .Q.s[w], "\n";
 }  

\d .

//Add extra initialisation parameters
.u.tick[src; 
    $[count tmp:.utils.getOpts["-tpLog"]; tmp; "tpLog"];
    $[count tmp:.utils.getOpts["-loggingDir"]; tmp; ""];
    $[count tmp:.utils.getOpts["-NAME"]; tmp; "tp"]
    ];

//Load in the extra logging script if required
.utils.extraLogs[];

\
 globals used
 .u.w - dictionary of tables->(handle;syms)
 .u.i - msg count in log file
 .u.j - total msg count (log file plus those held in buffer)
 .u.t - table names
 .u.L - tp log filename, e.g. `:./sym2008.09.11
 .u.l - handle to tp log file
 .u.d - date
 //Introduce extra globals
 .u.logH - handle to process logging file
 .u.tCount - dictionary of table names -> table counts
 .u.lastLogTime - Last time that the process log file was written to
