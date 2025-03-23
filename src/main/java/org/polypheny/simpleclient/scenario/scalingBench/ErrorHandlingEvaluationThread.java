/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/22/25, 1:33 PM The Polypheny Project
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

package org.polypheny.simpleclient.scenario.scalingBench;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.scenario.EvaluationThread;

@Slf4j
@Getter
public class ErrorHandlingEvaluationThread extends EvaluationThread {

    long numberOfFailedQueries = 0;
    Map<Class<? extends Throwable>, Set<String>> expectedExceptions;


    public ErrorHandlingEvaluationThread( Queue<QueryListEntry> queryList, Executor executor, Set<Integer> templateIds, boolean commitAfterEveryQuery, Map<Class<? extends Throwable>, Set<String>> expectedExceptions ) {
        super( queryList, executor, templateIds, commitAfterEveryQuery );
        this.expectedExceptions = expectedExceptions;
    }


    public boolean isExpectedException( Throwable e ) {
        for ( Map.Entry<Class<? extends Throwable>, Set<String>> entry : expectedExceptions.entrySet() ) {
            if ( entry.getKey().isInstance( e ) ) {
                return entry.getValue().stream().anyMatch( msg -> e.getMessage() != null && e.getMessage().contains( msg ) );
            }
        }
        return false;
    }


    @Override
    protected void executeAndMeasure( QueryListEntry queryListEntry ) {
        long startTime = System.nanoTime();
        try {
            executor.executeQuery( queryListEntry.query );

            long measuredTime = System.nanoTime() - startTime;
            measuredTimes.add( measuredTime );
            measuredTimePerQueryType.get( queryListEntry.templateId ).add( measuredTime );
            for ( Integer id : queryListEntry.templateIds ) {
                if ( !id.equals( queryListEntry.templateId ) ) {
                    measuredTimePerQueryType.get( id ).add( measuredTime );
                }
            }
        } catch ( ExecutorException e ) {
            throwIfUnexpected( e, "executing queries", queryListEntry.query.getSql() );
        }

    }


    @Override
    protected void commitSafely() {
        try {
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            throwIfUnexpected( e, "committing" );
        }
    }


    private void throwIfUnexpected( ExecutorException e, String operationDescription ) {
        throwIfUnexpected( e, operationDescription, null );
    }


    private void throwIfUnexpected( ExecutorException e, String operationDescription, String statement ) {
        if ( !isExpectedException( e ) ) {
            if ( statement != null ) {
                log.error( "Caught exception while {}. Statement: {}", operationDescription, statement, e );
            } else {
                log.error( "Caught exception while {}", operationDescription, e );
            }
            threadMonitor.notifyAboutError( e );
            rollbackSafely( e );
            throw new RuntimeException( e );
        }
        numberOfFailedQueries++;
    }

}
