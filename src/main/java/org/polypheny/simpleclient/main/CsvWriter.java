/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.polypheny.simpleclient.main;


import au.com.bytecode.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;


public class CsvWriter {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger( CsvWriter.class );

    private JSONParser parser;
    private CSVWriter writer;
    private volatile int queryNumber;


    CsvWriter( String path ) {
        try {
            writer = new CSVWriter( new FileWriter( path ), ';' );

            String[] entries = new String[]{ "Data Store", "Query Number", "Query Class", "Execution Time", "Total Time", "Measured Time" };
            writer.writeNext( entries );

            parser = new JSONParser();
            queryNumber = 0;
        } catch ( IOException e ) {
            LOGGER.error( "Exception while writing csv file", e );
        }
    }


    public synchronized void appendToCsv( String data, long measuredTime ) {
        try {
            queryNumber++;
            JSONObject jsonObject = (JSONObject) parser.parse( data );
            long queryClass = (long) jsonObject.get( "queryClass" );
            JSONArray jsonResults = (JSONArray) jsonObject.get( "results" );
            String[] entries;
            for ( Object object : jsonResults ) {
                JSONObject result = (JSONObject) object;
                String dataStore = (String) result.get( "dataStore" );
                long executionTime = (Long) result.get( "executionTime" );
                long totalTime = (Long) result.get( "totalTime" );
                entries = new String[]{ dataStore, "" + queryNumber, "" + queryClass, "" + executionTime, "" + totalTime, "" + measuredTime };
                writer.writeNext( entries );
            }
        } catch ( ParseException e ) {
            e.printStackTrace();
        }
    }


}


