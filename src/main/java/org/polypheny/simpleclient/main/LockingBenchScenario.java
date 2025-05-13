/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/21/25, 1:19 PM The Polypheny Project
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
import org.polypheny.simpleclient.scenario.lockingBench.LockingBench;
import org.polypheny.simpleclient.scenario.lockingBench.LockingBenchConfig;

@Slf4j
public class LockingBenchScenario {

    public static void schema( ExecutorFactory executorFactory, int sessionCount, int namespaceCount, int entityCount, double readWriteRatio ) {
        LockingBenchConfig config = new LockingBenchConfig( getProperties(sessionCount), sessionCount, namespaceCount, entityCount, readWriteRatio );
        LockingBench lockingBench = new LockingBench( executorFactory, config );
        lockingBench.createSchema( null, false );
    }


    public static void data( ExecutorFactory executorFactory, int sessionCount, int namespaceCount, int entityCount, double readWriteRatio ) {
        LockingBenchConfig config = new LockingBenchConfig( getProperties(sessionCount), sessionCount, namespaceCount, entityCount, readWriteRatio );
        LockingBench lockingBench = new LockingBench( executorFactory, config );
        ProgressReporter progressReporter = new ProgressBar( config.sessionCount, config.progressReportBase );
        lockingBench.generateData( null, progressReporter );
    }


    public static void warmup( ExecutorFactory executorFactory, int sessionCount, int namespaceCount, int entityCount, double readWriteRatio ) {
        LockingBenchConfig config = new LockingBenchConfig( getProperties(sessionCount), sessionCount, namespaceCount, entityCount, readWriteRatio );
        LockingBench lockingBench = new LockingBench( executorFactory, config );
        ProgressReporter progressReporter = new ProgressBar( config.sessionCount, config.progressReportBase );
        lockingBench.warmUp( progressReporter );
    }


    public static void workload( ExecutorFactory executorFactory, int sessionCount, int namespaceCount, int entityCount, double readWriteRatio ) {
        LockingBenchConfig config = new LockingBenchConfig( getProperties(sessionCount), sessionCount, namespaceCount, entityCount, readWriteRatio );
        LockingBench lockingBench = new LockingBench( executorFactory, config );
        ProgressReporter progressReporter = new ProgressBar( config.sessionCount, config.progressReportBase );
        final CsvWriter writer = new CsvWriter( "results.csv" );
        lockingBench.execute( progressReporter, writer, new File("."), config.sessionCount );
    }


    private static Properties getProperties(int sessionCount) {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/lockingBench/lockingbench.properties" ) ) );
            props.put("numberOfThreads", String.valueOf( sessionCount ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }

}
