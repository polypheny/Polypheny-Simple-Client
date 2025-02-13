/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2025 The Polypheny Project
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

package org.polypheny.simpleclient.scenario;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.JdbcExecutor;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryListEntry;

@Slf4j
public abstract class PolyphenyScenario extends Scenario {

    protected long executeRuntime;
    protected final Map<Integer, String> queryTypes;
    protected final List<Long> measuredTimes = Collections.synchronizedList( new LinkedList<>() );
    protected final Map<Integer, List<Long>> measuredTimePerQueryType = new ConcurrentHashMap<>();


    public PolyphenyScenario( JdbcExecutor.ExecutorFactory executorFactory, boolean commitAfterEveryQuery, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, commitAfterEveryQuery, dumpQueryList, queryMode );
        queryTypes = new HashMap<>();
    }


    protected long commonExecute( List<QueryListEntry> queryList, ProgressReporter progressReporter, File outputDirectory, int numberOfThreads, Function<Query, String> toString, Supplier<Executor> executor, Random random ) {
        Collections.shuffle( queryList, random );

        // This dumps the queries independent of the selected interface
        dumpQueryList( outputDirectory, queryList, toString );

        log.info( "Executing benchmark..." );
        (new Thread( new ProgressReporter.ReportQueryListProgress( queryList, progressReporter ) )).start();
        long startTime = System.nanoTime();

        List<EvaluationThread> threads = new ArrayList<>();
        for ( int i = 0; i < numberOfThreads; i++ ) {
            threads.add( new EvaluationThread( queryList, executor.get(), queryTypes.keySet(), commitAfterEveryQuery ) );
        }

        EvaluationThreadMonitor threadMonitor = new EvaluationThreadMonitor( threads );
        threads.forEach( t -> t.setThreadMonitor( threadMonitor ) );

        for ( EvaluationThread thread : threads ) {
            thread.start();
        }

        for ( EvaluationThread thread : threads ) {
            try {
                thread.join();
                this.measuredTimes.addAll( thread.getMeasuredTimes() );
                thread.getMeasuredTimePerQueryType().forEach( ( k, v ) -> {
                    if ( !this.measuredTimePerQueryType.containsKey( k ) ) {
                        this.measuredTimePerQueryType.put( k, new ArrayList<>() );
                    }
                    this.measuredTimePerQueryType.get( k ).addAll( v );
                } );
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
        properties.put( "measuredTime", calculateMean( measuredTimes ) );

        measuredTimePerQueryType.forEach( ( templateId, time ) -> calculateResults( queryTypes, properties, templateId, time ) );
        properties.put( "queryTypes_maxId", queryTypes.size() );
        properties.put( "executeRuntime", executeRuntime / 1000000000.0 );
        properties.put( "numberOfQueries", measuredTimes.size() );
        properties.put( "throughput", measuredTimes.size() / (executeRuntime / 1000000000.0) );
    }


    private void dumpQueryList( File outputDirectory, List<QueryListEntry> queryList, Function<Query, String> toString ) {
        if ( outputDirectory != null && dumpQueryList ) {
            log.info( "Dump query list..." );
            try {
                FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "queryList" );
                queryList.forEach( query -> {
                    try {
                        fw.append( toString.apply( query.query ) ).append( "\n" );
                    } catch ( IOException e ) {
                        log.error( "Error while dumping query list", e );
                    }
                } );
                fw.close();
            } catch ( IOException e ) {
                log.error( "Error while dumping query list", e );
            }
        }

    }

}
