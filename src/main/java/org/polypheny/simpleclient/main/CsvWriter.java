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


import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CsvWriter {

    private AtomicInteger queryNumber;
    private CSVWriter writer;


    CsvWriter( String path ) {
        try {
            queryNumber = new AtomicInteger();
            writer = new CSVWriter( new FileWriter( path ) );
            String[] entries = new String[]{ "Number", "Measured Time", "Query" };
            writer.writeNext( entries );
        } catch ( IOException e ) {
            log.error( "Exception while writing csv file", e );
        }
    }


    public void appendToCsv( String query, long measuredTime ) {
        writer.writeNext( new String[]{ queryNumber.incrementAndGet() + "", "" + measuredTime, query } );
    }


    public void flush() throws IOException {
        writer.flush();
    }

}
