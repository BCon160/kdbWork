#!/usr/bin/perl

#Author: Brendan Connolly
#Description: Script to startup or stop q processes in the tickSetup as per Advanced Kdb+ CMTP
#Note: I realise that this should all be in subroutines but as this is only a dummy setup, I haven't bothered
#Note: This script assumes a standard linux (QHOME=~/q) 64 bit q install.  Obviuosly this is not good practice but for the sake of this script, I will save my self a fair bit of hassle by simply making assumptions about the environment in which it is run

use strict;
use warnings;
use 5.010;
use Getopt::Long qw(GetOptions);
use List::MoreUtils qw(first_index);

#Declare variables that will be used for configuration
my $testMode;
my $help;
my $start;
my $stop;
my $tpPortNum = 5010;
my $otherPortNum = 5011;
my $schemaLocation = 'sym';
my $logLocation = './logging/';
my $rdbMode = 'sharded';
my $dbLocation = 'db/';
my $extraLogging;
my $usageStr = "Usage: $0 \n
    --START <process/All> \n
    --STOP <process/All> \n
    Available processes:
    fh (Mock Feedhandler)
    cep (Computes Derived Tables)
    tp (Tickerplant)
    rdb (All purpose real-time database)
    rdbRaw (Real-time database stores raw tables only)
    rdbDerived (Real-time database stores derived tables only) \n
    --TEST ->Gives information on currently running processes\n
    --SRC <schemaLocation> ->Name of schema q file, path is assumed to be tick/ and .q extension is automatically added\n
    --TP_PORT <tpPortNumber> ->TP always gets it's own port as it has to be running for the other processes to initialise properly\n
    --PORT_START <nonTpStartPort> ->All non-tp processes will be started on this and subsequent ports\n
    --LOGDIR <logMessageDirectory> ->Internal log files directory\n
    --RDB_MODE <sharded/single> ->Sharded will start rdbDerived and rdbRaw, single will start just rdb\n
    --HDBDIR <dataDirectory> -> Historical data directory\n
    --EXTRALOGGING ->Enable logging of connections on all processes\n\n";
#Parse options off the command line
GetOptions(
    'START=s' => \$start,
    'STOP=s' => \$stop,
    'TEST' => \$testMode,
    'SRC=s' => \$schemaLocation,
    'TP_PORT=s' => \$tpPortNum,
    'PORT_START=s' => \$otherPortNum,
    'LOGDIR=s' => \$logLocation,
    'RDB_MODE=s' => \$rdbMode,
    'HDBDIR=s' => \$dbLocation,
    'EXTRALOGGING' => \$extraLogging,
    'help' => \$help
) or die $usageStr;

if($help) {
    print $usageStr;
}

#If start
if($start) {
    #Initialise command string list
    my @commands = ("tp", "~/q/l64/q ./tickBC.q", "fh", "~/q/l64/q ./feed.q", "cep", "~/q/l64/q ./CEP.q");

    #Create two separate run commands if the user wants raw rdb and derived rdb
    if ($start eq 'All') {
        if($rdbMode eq 'sharded') {
            push(@commands, ("rdbRaw", "~/q/l64/q ./tick/rBC.q", "rdbDerived", "~/q/l64/q ./tick/rBC.q"));
        }
        else {
            push(@commands, ("rdb", "~/q/l64/q ./tick/rBC.q"));
        }
    }
    #If user is starting specific process, need all commands available
    else {
            push(@commands, ("rdbRaw", "~/q/l64/q ./tick/rBC.q", "rdbDerived", "~/q/l64/q ./tick/rBC.q", "rdb", "~/q/l64/q ./tick/rBC.q"));
    }

    #If we are not starting all processes pick the correct one from the list
    if($start ne 'All') {
        my $index = first_index {/$start/} @commands;
        @commands = ($commands[$index], $commands[$index + 1]);
    }
    
    #Change to dictionary (process -> startCommand)
    my %commands_hash = @commands;

    foreach my $key (keys %commands_hash) {
        my $value = $commands_hash{$key};
        
        #Add schema file name to those processes that need it
        if (($key eq 'tp') or ($key eq 'cep')) {
            $value = $value . " " . $schemaLocation;
        }

        #Add tp connection details to processes that need them
        if ($key ne 'tp') {
            $value = $value . " :" . $tpPortNum;
        }
        
        #Add table subscription lists to RDB commands if running in shared mode
        if ($key eq 'rdbRaw') {
            $value = $value . " -SUB_TABLES trade,quote ";
        }
        if ($key eq 'rdbDerived') {
            $value = $value . " -SUB_TABLES marketSummary ";
        }

        #Add Process Name for identification purposes
        $value = $value . " -NAME " . $key;

        #Add port numbers
        if($key eq 'tp') {
            $value = $value . " -p " . $tpPortNum;
        }
        else {
            $value = $value . " -p " . $otherPortNum;
            $otherPortNum = $otherPortNum + 1;
        }

        #Add logdir
        $value = $value . " -loggingDir " . $logLocation;
        
        #Add hdbdir (if applicable)
        if($key =~ /^rdb/) {
            $value = $value . " -HDB_DIR " . $dbLocation;
        }
        
        if($extraLogging) {
            $value = $value . " -EXTRALOGGING";
        }

        #Make sure the processes are running in the backgroud
        $value = $value . " &";
        $commands_hash{$key} = $value;
    }

    #Have to start tp first if starting all
    if($start eq 'All') {
        startProc('tp', $commands_hash{'tp'});
        delete $commands_hash{'tp'};
    }

    #Perform system commands - Taking out of main loop for debugging
    foreach my $key (keys %commands_hash) {
        startProc($key, $commands_hash{$key})
    }
}

#If stop or test
if($stop or $testMode) {
    my @raw_details = split (/\n/, `ps -eo pid,cmd`);
    my %details;
    #Get details of running q processes
    foreach my $proc ('tp', 'rdbRaw', 'rdbDerived', 'rdb', 'fh', 'cep') {
        my @tmp = grep {$_ =~ /-NAME $proc /} @raw_details;
        $details{$proc} = $tmp[0];
    }

    #Perform rdb type check, we don't want to report on both sharded and non-sharded instances
    if (defined($details{"rdb"})) {
        delete @details{'rdbRaw', 'rdbDerived'};
    }
    else {
        delete $details{'rdb'};
    }

    #If test - Report these
    if($testMode) {
        foreach my $key (keys %details) {
            my $value = $details{$key};
            if(not $value) {
                $value = " ";
            }
            if($value =~ /q /) {
                #left trim output
                $value =~ s/^\s+//; 
                print "$key process is running\n";
                print "PID RUNCOMMAND\n";
                print $value;
                print "\n\n";
            }
            else {
                print "$key process is not running\n\n";
            }
        }
    }
    #If stop - shut these down
    if($stop) {
        #Perform system commands
        if($stop eq 'All') {
            foreach my $key (keys %details) {
                killProc($key, $details{$key});
            }
        }
        else {
            killProc($stop, $details{$stop});
        }
    }
}

sub killProc {
    my ($proc_name, $proc_details) = @_;
    if (not $proc_details) {
        $proc_details = ' ';
    }
    if ($proc_details =~ /q /) {
        #left trim before splitting to make sure that the indices are correct
        $proc_details =~ s/^\s+//;
        my @tmp = split(/ /, $proc_details);
        my $PID = $tmp[0];
        my $result = system("kill $PID");
        if ($result == 0) {
            print "Successfully shut down $proc_name\n";
        }
        else {
            print "Error shutting down process\n";
        }
    }
    else {
        print "Not shutting down $proc_name as it isn't running\n";
    }
}

sub startProc {
    my ($proc_name, $command) = @_;
    my $result = system($command);
    #Tell the user the result of the start up command
    if ($result == 0){
        print "Successfully started $proc_name\n"
    }
    else {
        print "Error starting $proc_name\n"
    }
    sleep(2);
}
