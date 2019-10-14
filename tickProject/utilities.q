//Define dunctions that will be used across all processes
//Author: Brendan Connolly

\d .utils
//Get command line options function
getOpts:{[opt]
    i:first where .z.x like opt;
    .z.x[i+1]
 };

//Load in the extra logging script if specified on the command line
extraLogs:{
    if[any .z.x like "-EXTRALOGGING";
        value"\\l logging.q"
    ];
 };

\d .
