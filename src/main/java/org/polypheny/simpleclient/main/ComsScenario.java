/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/15/23, 4:22 PM The Polypheny Project
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
 */

package org.polypheny.simpleclient.main;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.cli.Mode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.coms.Coms;
import org.polypheny.simpleclient.scenario.coms.ComsConfig;

@Slf4j
public class ComsScenario {


    public static void data( ExecutorFactory executorFactory, Mode mode, int multiplier ) {
        ComsConfig config = new ComsConfig( mode.toString(), getProperties() );
        Coms bench = new Coms( executorFactory, config, true, false, QueryMode.TABLE );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        bench.generateData( null, progressReporter );
    }


    public static void workload( ExecutorFactory executorFactory, Mode mode, int multiplier, boolean writeCsv ) {
        ComsConfig config = new ComsConfig( mode.toString(), getProperties() );
        Coms bench = new Coms( executorFactory, config, true, false, QueryMode.TABLE );

        final CsvWriter csvWriter;
        if ( true ) {
            csvWriter = new CsvWriter( "results.csv" );
        } else {
            csvWriter = null;
        }
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        bench.execute( progressReporter, csvWriter, new File( "." ), config.numberOfThreads );

    }


    public static void schema( ExecutorFactory executorFactory, Mode mode ) {
        ComsConfig config = new ComsConfig( mode.toString(), getProperties() );
        Coms bench = new Coms( executorFactory, config, true, false, QueryMode.TABLE );
        bench.createSchema( null, true );
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/coms/coms.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }

}