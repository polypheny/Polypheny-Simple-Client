/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/24/23, 11:58 AM The Polypheny Project
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.scenario.graph.GraphBench.EvaluationThreadMonitor;

@Getter
@Slf4j
public class EvaluationThread extends Thread {

    private final Executor executor;
    private final List<QueryListEntry> queries;
    private boolean abort = false;
    @Setter
    private EvaluationThreadMonitor threadMonitor;

    private final List<Long> measuredTimes = Collections.synchronizedList( new LinkedList<>() );

    private final Map<Integer, List<Long>> measuredTimePerQueryType = new ConcurrentHashMap<>();

    final boolean commitAfterEveryQuery;


    public EvaluationThread( List<QueryListEntry> queryList, Executor executor, Set<Integer> templateIds, boolean commitAfterEveryQuery ) {
        super( "EvaluationThread" );
        this.executor = executor;
        this.queries = queryList;
        templateIds.forEach( id -> measuredTimePerQueryType.put( id, new ArrayList<>() ) );
        this.commitAfterEveryQuery = commitAfterEveryQuery;
    }


    @Override
    public void run() {
        long measuredTimeStart;
        long measuredTime;
        QueryListEntry queryListEntry;

        while ( !queries.isEmpty() && !abort ) {
            measuredTimeStart = System.nanoTime();
            try {
                queryListEntry = queries.removeFirst();
            } catch ( IndexOutOfBoundsException e ) { // This is neither nice nor efficient...
                // This can happen due to concurrency if two threads enter the while-loop and there is only one thread left
                // Simply leaf the loop
                break;
            }
            try {
                executor.executeQuery( queryListEntry.query );
            } catch ( ExecutorException e ) {
                log.error( "Caught exception while executing queries", e );
                threadMonitor.notifyAboutError( e );
                try {
                    executor.executeRollback();
                } catch ( ExecutorException ex ) {
                    log.error( "Error while rollback", e );
                }
                throw new RuntimeException( e );
            }
            measuredTime = System.nanoTime() - measuredTimeStart;
            measuredTimes.add( measuredTime );
            measuredTimePerQueryType.get( queryListEntry.templateId ).add( measuredTime );
            for ( Integer id : queryListEntry.templateIds ) {
                if ( id != queryListEntry.templateId ) {
                    measuredTimePerQueryType.get( id ).add( measuredTime );
                }
            }
            if ( commitAfterEveryQuery ) {
                try {
                    executor.executeCommit();
                } catch ( ExecutorException e ) {
                    log.error( "Caught exception while committing", e );
                    threadMonitor.notifyAboutError( e );
                    try {
                        executor.executeRollback();
                    } catch ( ExecutorException ex ) {
                        log.error( "Error while rollback", e );
                    }
                    throw new RuntimeException( e );
                }
            }
        }

        try {
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            log.error( "Caught exception while committing", e );
            threadMonitor.notifyAboutError( e );
            try {
                executor.executeRollback();
            } catch ( ExecutorException ex ) {
                log.error( "Error while rollback", e );
            }
            throw new RuntimeException( e );
        }

        executor.flushCsvWriter();
    }


    public void abort() {
        this.abort = true;
    }


    public void closeExecutor() {
        Scenario.commitAndCloseExecutor( executor );
    }

}
