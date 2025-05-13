/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/22/25, 1:27 PM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.lockingBench;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.scenario.EvaluationThread;
import org.polypheny.simpleclient.scenario.EvaluationThreadMonitor;
import org.polypheny.simpleclient.scenario.PolyphenyScenario;

@Slf4j
public abstract class ErrorHandlingPolyphenyScenario extends PolyphenyScenario {
    protected long failedQueries = 0;

    public ErrorHandlingPolyphenyScenario( ExecutorFactory executorFactory, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, queryMode );
    }


    protected long commonExecuteWithExpectedErrors( List<QueryListEntry> queries, ProgressReporter progressReporter, File outputDirectory, int numberOfThreads, Function<Query, String> toString, Supplier<Executor> executor, Random random, Map<Class<? extends Throwable>, Set<String>> expectedExceptions ) {
        Collections.shuffle( queries, random );

        // This dumps the queries independent of the selected interface
        dumpQueryList( outputDirectory, queries, toString );

        Queue<QueryListEntry> queryList = new ConcurrentLinkedQueue<>( queries );

        log.info( "Executing benchmark..." );
        (new Thread( new ProgressReporter.ReportQueryListProgress( queryList, progressReporter ) )).start();
        long startTime = System.nanoTime();

        List<ErrorHandlingEvaluationThread> threads = new ArrayList<>();
        for ( int i = 0; i < numberOfThreads; i++ ) {
            threads.add( new ErrorHandlingEvaluationThread( queryList, executor.get(), queryTypes.keySet(), commitAfterEveryQuery, expectedExceptions ) );
        }

        EvaluationThreadMonitor threadMonitor = new EvaluationThreadMonitor( threads );
        threads.forEach( t -> t.setThreadMonitor( threadMonitor ) );

        for ( ErrorHandlingEvaluationThread thread : threads ) {
            thread.start();
        }

        for ( ErrorHandlingEvaluationThread thread : threads ) {
            try {
                thread.join();
                this.measuredTimes.addAll( thread.getMeasuredTimes() );
                thread.getMeasuredTimePerQueryType().forEach( ( k, v ) -> {
                    if ( !this.measuredTimePerQueryType.containsKey( k ) ) {
                        this.measuredTimePerQueryType.put( k, new ArrayList<>() );
                    }
                    this.measuredTimePerQueryType.get( k ).addAll( v );
                } );
                this.failedQueries += thread.getNumberOfFailedQueries();
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }

        executeRuntime = System.nanoTime() - startTime;

        for ( EvaluationThread thread : threads ) {
            thread.closeExecutor();
        }

        if ( threadMonitor.isAborted() ) {
            throw new RuntimeException( "Exception while executing benchmark", threadMonitor.getException() );
        }

        log.info( "run time: {} s", executeRuntime / 1000000000 );

        return executeRuntime;
    }

    @Override
    public void analyze( Properties properties, File outputDirectory ) {
        properties.put( "executeRuntime [s]", executeRuntime / 1000000000.0 );
        properties.put( "measuredTime [ms]", calculateMean( measuredTimes ) );

        properties.put( "numberOfQueries", measuredTimes.size());
        properties.put( "numberOfSuccessfulQueries", measuredTimes.size() - failedQueries);
        properties.put( "numberOfFailedQueries", failedQueries );
        properties.put("successRatio", calculateSuccessRatio() );

        properties.put( "throughput [succ.q/s]", measuredTimes.size() / (executeRuntime / 1000000000.0) );

        measuredTimePerQueryType.forEach( ( templateId, time ) -> calculateResults( queryTypes, properties, templateId, time ) );
        properties.put( "queryTypes_maxId", queryTypes.size() );
    }

    private double calculateSuccessRatio() {
        double unroundedRatio = (1.0 * measuredTimes.size() - failedQueries) / measuredTimes.size();
        return Math.round( unroundedRatio * 100.0 ) / 100.0;
    }

    public void storeIndividualExecutionTimes(File outputDirectory, String baseFileName) {
        if (!outputDirectory.exists()) {
            outputDirectory.mkdirs();
        }

        measuredTimePerQueryType.forEach((templateId, times) -> {
            File outputFile = new File(outputDirectory, baseFileName + "_template" + templateId + ".txt");

            try ( BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                for (Long time : times) {
                    writer.write(time.toString());
                    writer.newLine();
                }
            } catch ( IOException e) {
                throw new RuntimeException( "Exception while exporting individual execution times.", e );
            }
        });
    }


}
