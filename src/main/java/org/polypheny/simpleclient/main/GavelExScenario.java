/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 The Polypheny Project
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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.gavelNG.GavelNG;
import org.polypheny.simpleclient.scenario.gavelNG.GavelNGConfig;
import org.polypheny.simpleclient.scenario.gavelNG.GavelNGProfile;


@Slf4j
public class GavelExScenario {

    public static void schema( ExecutorFactory executorFactoryHSQLDB, ExecutorFactory executorFactoryMONGODB, boolean commitAfterEveryQuery, QueryMode queryMode ) {
        GavelNGConfig config = new GavelNGConfig( getProperties(), 1);
        GavelNGProfile profile = new GavelNGProfile( getProfileProperties() );

        GavelNG gavelNG = new GavelNG( executorFactoryHSQLDB, executorFactoryMONGODB, config, profile, commitAfterEveryQuery, false, queryMode);
        gavelNG.createSchema( true );
    }


    public static void data( ExecutorFactory executorFactoryHSQLDB, ExecutorFactory executorFactoryMONGODB, int multiplier, boolean commitAfterEveryQuery, QueryMode queryMode ) {
        GavelNGConfig config = new GavelNGConfig( getProperties(), multiplier );
        GavelNGProfile profile = new GavelNGProfile( getProfileProperties() );
        GavelNG gavelNG = new GavelNG( executorFactoryHSQLDB, executorFactoryMONGODB, config, profile, commitAfterEveryQuery, false, queryMode );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        gavelNG.generateData( progressReporter );
    }


    public static void workload( ExecutorFactory executorFactoryHSQLDB, ExecutorFactory executorFactoryMONGODB, int multiplier, boolean commitAfterEveryQuery, boolean writeCsv, boolean dumpQueryList, QueryMode queryMode ) {
        GavelNGConfig config = new GavelNGConfig( getProperties(), multiplier );
        GavelNGProfile profile = new GavelNGProfile( getProfileProperties() );
        GavelNG gavelNG = new GavelNG( executorFactoryHSQLDB, executorFactoryMONGODB, config, profile, commitAfterEveryQuery, dumpQueryList, queryMode );

        final CsvWriter csvWriter;
        if ( writeCsv ) {
            csvWriter = new CsvWriter( "results.csv" );
        } else {
            csvWriter = null;
        }
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );

        gavelNG.execute( progressReporter, csvWriter, new File( "." ), config.numberOfThreads );
    }


    public static void warmup( ExecutorFactory executorFactoryHSQLDB, ExecutorFactory executorFactoryMONGODB, int multiplier, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        GavelNGConfig config = new GavelNGConfig( getProperties(), 1 );
        GavelNGProfile profile = new GavelNGProfile( getProfileProperties() );
        GavelNG gavelNG = new GavelNG( executorFactoryHSQLDB, executorFactoryMONGODB, config, profile, commitAfterEveryQuery, dumpQueryList, queryMode );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );

        gavelNG.warmUp( progressReporter, config.numberOfWarmUpIterations );
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavelNG/gavelEx.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }


    private static Properties getProfileProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavelNG/gavelExProfile1.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }

}
