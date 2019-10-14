//Title: loadCSV.java
//Author: Brendan Connolly
//Date: 13/01/18
//Description: Takes a csv file, reads it into string form, casts to the correct types and publishes results to kdb+ tickerplant

//Running notes:
//To compile javac loadCSV.java -classpath javakdb/src/
//To run: java -cp .:javakdb/src/ loadCSV test.csv trade -tpPort 5010 -head

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.io.IOException;
import kx.c;
import kx.c.KException;

public class loadCSV {

    public static void main(String[] args) {
        //Give help if required        
        if(args[0].equals("-help")) {
            System.out.print("Usage:java -cp .:<pathToCompiledkxPackage> loadCSV \n\t Required Args:\n\n\t filePath -> Path to csv for loading\n\n\t tableName -> Name of table in kdb+\n\n\t Optional Args:\n\n\t --tpPort -> Port on which kdb+ tickerplant is running (default is 5010)\n\n\t --head -> If the headers are given in the csv file, provide this flag\n\n");
            System.exit(0);
        }

        //Read positional command line args
        String filePath = args[0];
        String tableName = args[1];
        
        //Read optional command line args
        int tpPort = 5010;
        boolean head = false;
        for (int i = 2; i < args.length; i++) {
            if(args[i].equals("-tpPort"))
                tpPort = Integer.parseInt(args[i+1]);
            else if(args[i].equals("-head"))
                head = true;
            else{}
        }

        //Instanciate required data structures
        File file= new File(filePath);
        List<List<String>> stringVals = new ArrayList<>();
        Scanner inputStream;

        //Read CSV to string form
        try{
            //Open file stream
            inputStream = new Scanner(file);

            //Make sure to skip the headers if they are present
            if (head) {
                inputStream.next();
            }

            //Loop through the file and split each line on ","
            //Add the split line to stringVals
            while(inputStream.hasNext()){
                String line= inputStream.next();
                String[] values = line.split(",");
                stringVals.add(Arrays.asList(values));
            }

            //Close file stream
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        
        try {
            //Connect to kdb+ tickerplant and get schema types
            c c=new c("localhost", tpPort);
            char[] types = (char[]) c.k("exec t from meta " + tableName);
            
            //Initialise data structure that will hold the casted data
            Object[] typedVals = new Object[types.length]; 
            
            //Make call to cast the strings to the correct type
            for (int i = 0; i < stringVals.get(0).size(); i++) {
                typedVals[i] = cast(stringVals, i, types[i]).toArray();
            }
            
            //Publish to tickerplant and close connection
            c.k(".u.upd", tableName, typedVals);
            c.close();
        }
        catch (KException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Takes a string list and outputs a typed list depending on the typeC parameter
    //Params:
    //strVals - list of lists of strings cotaining raw form of a csv file
    //colIdx - Gives the column in strVals that we are currently converting
    //typeC - Gives type that we want to convert to    

    //Note: I'm only supporting the types that are represented in my tick setup
    private static List<?> cast(List<List<String>> strVals, int colIdx, Character typeC) {
        //kdb+ float
        if (typeC.equals('f')) {
            List<Double> doubleList = new ArrayList<Double>();
            for (int j = 0; j < strVals.size(); j++) {
                doubleList.add(Double.parseDouble(strVals.get(j).get(colIdx)));
            }
            return doubleList;
        }
        //kdb+ long
        else if (typeC.equals('j')) {
            List<Long> longList = new ArrayList<Long>();
            for (int j = 0; j < strVals.size(); j++) {
                longList.add(Long.parseLong(strVals.get(j).get(colIdx)));
            }
            return longList;
        }
        //kdb+ symbol
        else if (typeC.equals('s')) {
            List<String> stringList = new ArrayList<String>();
            for (int j = 0; j < strVals.size(); j++) {
                stringList.add(strVals.get(j).get(colIdx));
            }
            return stringList;
        }
        //kdb+ timespan
        else if (typeC.equals('n')) {
            List<Object> tsList = new ArrayList<Object>();
            for (int j = 0; j < strVals.size(); j++) {
                tsList.add(new c.Timespan(parseTimespan(strVals.get(j).get(colIdx))));
            }
            return tsList;
        }
        //Bad practice: should have a "type not supported" catch here really
        else{
            return new ArrayList<Object>();
        }
    }

    //Function to change input string to nanoseconds
    //Input: inpStr<String> has form: "0Dxx:xx:xx.xxxxxxxxx"
    private static long parseTimespan(String inpStr) {
        inpStr = inpStr.replace('.', ':');
        inpStr = inpStr.replace("0D", "");
        String[] tmp = inpStr.split(":");
        return (Long.parseLong(tmp[0]) * 3600000000000L) + (Long.parseLong(tmp[1]) * 60000000000L) + (Long.parseLong(tmp[2]) * 1000000000L) + (Long.parseLong(tmp[3]));
    }
}
