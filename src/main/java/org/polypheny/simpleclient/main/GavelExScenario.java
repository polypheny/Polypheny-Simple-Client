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
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.ProfileSelector;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.gavelEx.GavelEx;
import org.polypheny.simpleclient.scenario.gavelEx.GavelExConfig;
import org.polypheny.simpleclient.scenario.gavelEx.GavelExProfile;
import org.polypheny.simpleclient.scenario.gavelEx.GavelExSettings;


@Slf4j
public class GavelExScenario {

    public static void schema( ExecutorFactory executorFactoryHSQLDB, ExecutorFactory executorFactoryMONGODB, boolean commitAfterEveryQuery, QueryMode queryMode ) {
        GavelExConfig config = new GavelExConfig( getProperties(), 1 );
        GavelExSettings gavelExSettings = new GavelExSettings( getProfileProperties(), executorFactoryHSQLDB );
        GavelEx gavelEx = new GavelEx( executorFactoryHSQLDB, executorFactoryMONGODB, config, commitAfterEveryQuery, false, queryMode );
        gavelEx.createSchema( true, gavelExSettings );
    }


    public static void data( ExecutorFactory executorFactoryHSQLDB, ExecutorFactory executorFactoryMONGODB, int multiplier, boolean commitAfterEveryQuery, QueryMode queryMode ) {
        GavelExConfig config = new GavelExConfig( getProperties(), multiplier );
        GavelEx gavelEx = new GavelEx( executorFactoryHSQLDB, executorFactoryMONGODB, config, commitAfterEveryQuery, false, queryMode );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        gavelEx.generateData( progressReporter );
    }


    public static void workload( ExecutorFactory executorFactoryHSQLDB, ExecutorFactory executorFactoryMONGODB, int multiplier, boolean commitAfterEveryQuery, boolean writeCsv, boolean dumpQueryList, QueryMode queryMode, ProfileSelector profileSelector ) {
        GavelExConfig config = new GavelExConfig( getProperties(), multiplier );
        GavelEx gavelEx = new GavelEx( executorFactoryHSQLDB, executorFactoryMONGODB, config, commitAfterEveryQuery, dumpQueryList, queryMode );

        final CsvWriter csvWriter;
        if ( writeCsv ) {
            csvWriter = new CsvWriter( "results.csv" );
        } else {
            csvWriter = null;
        }
        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        GavelExProfile profile = new GavelExProfile( getProfileProperties() );
        gavelEx.execute( progressReporter, csvWriter, new File( "." ), config.numberOfThreads, profile );
    }


    public static void warmup( ExecutorFactory executorFactoryHSQLDB, ExecutorFactory executorFactoryMONGODB, int multiplier, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        GavelExConfig config = new GavelExConfig( getProperties(), 1 );
        GavelEx gavelEx = new GavelEx( executorFactoryHSQLDB, executorFactoryMONGODB, config, commitAfterEveryQuery, dumpQueryList, queryMode );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        gavelEx.warmUp( progressReporter, config.numberOfWarmUpIterations );
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavelEx/gavelEx.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }


    private static Properties getProfileProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/gavelEx/gavelExProfile1.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }


}
