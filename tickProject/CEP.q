//// CEP.q ////
//Author: Brendan Connolly
//Description: Mock CEP for the Advanced kdb+ exam.  Subscribes to trade and quote tables from the tp, publishes an aggregated table back to tp

//Usage:
/q CEP.q SRC [host]:port[:usr:pwd] [-p portNumber]
//Get schemas
system"l tick/",(src:first .z.x,enlist"sym"),".q"

\l utilities.q

//Define upd function
upd:{[t;x]
    .Q.dd[`.cep;t] insert x
 };

//Initialise the various variables needed for this CEP to function
//Do this from the root namespace as I need to access root namespace variables
.cep.init:{
    .cep.trade:trade;
    .cep.quote:quote;
    .cep.tp:hopen `$":", .z.x[1],(-1+count .z.x)_enlist(":5010");
    .cep.tp(`.u.sub;`trade`quote;`);
 };

\d .cep

//agg func
agg:{
    //Generate trade aggs then join on quote aggs
    aggTab:select min price, max price, sum size by sym from trade;
    aggTab:aggTab uj select max bid, min ask by sym from quote;
    aggTab
 };

//pub func
pub:{
    //Generate aggregated table
    aggTab:agg[];
    //Publish asynchronously to the marketSummary table on the tp
    neg[tp](`.u.upd; `marketSummary; value flip 0!aggTab);
    //Make sure there are no memory leaks
    cleanUp[];
 };

//cleanup func
cleanUp:{
    //Once an agg has been generated, we don't need any of the source data anymore.  Therefore delete it all
    delete from `trade;
    delete from `quote;
 };

\d .

//Define .u.end so that an error isn't thrown at eod on the tp
.u.end:{(::)};

//timer func
.z.ts:{.cep.pub[]};

.cep.init[];

//Publish aggs every 10 seconds
system"t 10000";

//Load in the extra logging script if required
.utils.extraLogs[];

//Globals used:
// .cep.trade - copy of the trade table in the .cep context
// .cep.quote - copy of the quote table in the .cep context
// .cep.tp - handle to tp for publishing
