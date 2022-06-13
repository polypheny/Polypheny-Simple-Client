/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2022 The Polypheny Project
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
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.scenario.docbench.DocBench;
import org.polypheny.simpleclient.scenario.docbench.DocBenchConfig;


@Slf4j
public class DocBenchScenario {

    public static void schema( ExecutorFactory executorFactory, boolean commitAfterEveryQuery ) {
        DocBenchConfig config = new DocBenchConfig( getProperties(), 1 );
        DocBench DocBench = new DocBench( executorFactory, config, commitAfterEveryQuery, false );
        DocBench.createSchema( true );
    }


    public static void data( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery ) {
        DocBenchConfig config = new DocBenchConfig( getProperties(), multiplier );
        DocBench DocBench = new DocBench( executorFactory, config, commitAfterEveryQuery, false );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        DocBench.generateData( progressReporter );
    }


    public static void workload( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery, boolean writeCsv, boolean dumpQueryList ) {
        DocBenchConfig config = new DocBenchConfig( getProperties(), multiplier );
        DocBench DocBench = new DocBench( executorFactory, config, commitAfterEveryQuery, dumpQueryList );

        final CsvWriter csvWriter;
        if ( writeCsv ) {
            csvWriter = new CsvWriter( "results.csv" );
        } else {
            csvWriter = null;
        }

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        DocBench.execute( progressReporter, csvWriter, new File( "." ), config.numberOfThreads );
    }


    public static void warmup( ExecutorFactory executorFactory, int multiplier, boolean commitAfterEveryQuery, boolean dumpQueryList ) {
        DocBenchConfig config = new DocBenchConfig( getProperties(), multiplier );
        DocBench DocBench = new DocBench( executorFactory, config, commitAfterEveryQuery, dumpQueryList );

        ProgressReporter progressReporter = new ProgressBar( config.numberOfThreads, config.progressReportBase );
        DocBench.warmUp( progressReporter );
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/docbench/docbench.properties" ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }

}
