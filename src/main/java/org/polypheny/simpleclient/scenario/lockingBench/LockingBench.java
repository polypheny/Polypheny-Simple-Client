/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/21/25, 1:28 PM The Polypheny Project
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.polypheny.simpleclient.query.QueryListEntry;
import org.polypheny.simpleclient.scenario.lockingBench.NumberTracker.Mode;
import org.polypheny.simpleclient.scenario.lockingBench.queryBuilder.FullRead;
import org.polypheny.simpleclient.scenario.lockingBench.queryBuilder.RangeRead;
import org.polypheny.simpleclient.scenario.lockingBench.queryBuilder.RangeUpdate;
import org.polypheny.simpleclient.scenario.lockingBench.queryBuilder.SingleDelete;
import org.polypheny.simpleclient.scenario.lockingBench.queryBuilder.SingleInsert;
import org.polypheny.simpleclient.scenario.lockingBench.queryBuilder.SingleRead;
import org.polypheny.simpleclient.scenario.lockingBench.queryBuilder.SingleUpdate;


@Slf4j
public class LockingBench extends ErrorHandlingPolyphenyScenario {

    private final LockingBenchConfig config;
    private final Map<Class<? extends Throwable>, Set<String>> expectedExceptions;


    public LockingBench( ExecutorFactory executorFactory, LockingBenchConfig config ) {
        super( executorFactory, true, false, QueryMode.TABLE );
        this.config = config;
        this.expectedExceptions = initializeExpectedExceptions();
    }


    public Map<Class<? extends Throwable>, Set<String>> initializeExpectedExceptions() {
        return ImmutableMap.<Class<? extends Throwable>, Set<String>>builder()
                .put( ExecutorException.class, ImmutableSet.of(
                        "Committing current transaction failed: Rolling back due to MVCC write conflict.",
                        "encountered a deadlock while acquiring a lock of type"
                ) )
                .build();
    }


    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        log.info( "Creating schema..." );
        if ( !(executorFactory instanceof PolyphenyDbJdbcExecutorFactory) ) {
            throw new RuntimeException( "Unsupported executor factory: " + executorFactory.getClass().getName() );
        }

        Executor executor = executorFactory.createExecutorInstance();
        LockingBenchSchema schema = new LockingBenchSchema( config );
        try {
            for ( Query createNamespaceQuery : schema.getCrateNamespaceQueries() ) {
                executor.executeQuery( createNamespaceQuery );
            }

            for ( Query query : schema.getCrateTableQueries() ) {
                executor.executeQuery( query );
            }
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        log.info( "Generating data..." );
        Executor executor = executorFactory.createExecutorInstance();
        NumberTracker numberTracker = new NumberTracker( config, Mode.SETUP );
        LockingBenchDataGenerator dataGenerator = new LockingBenchDataGenerator( config, executor, progressReporter, numberTracker );
        LockingBenchSchema schema = new LockingBenchSchema( config );

        try {
            dataGenerator.generateEntries( schema.getEntities() );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while generating data", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        NumberTracker numberTracker = new NumberTracker( config, Mode.RUN );
        LockingBenchSchema schema = new LockingBenchSchema( config );

        log.info( "Calculating query type distribution..." );
        Map<String, Integer> distribution = distributeQueries( config.numberOfQueries, config.readWriteRatio );

        log.info( "Preparing query list for the benchmark..." );
        List<QueryListEntry> queryList = new Vector<>();
        addNumberOfTimes( queryList, new SingleRead( schema, numberTracker ), distribution.get( "singleRead" ) );
        addNumberOfTimes( queryList, new RangeRead( schema, numberTracker ), distribution.get( "rangeRead" ) );
        addNumberOfTimes( queryList, new FullRead( schema ), distribution.get( "fullRead" ) );
        addNumberOfTimes( queryList, new SingleUpdate( schema, numberTracker ), distribution.get( "singleUpdate" ) );
        addNumberOfTimes( queryList, new RangeUpdate( schema, numberTracker ), distribution.get( "rangeUpdate" ) );
        addNumberOfTimes( queryList, new SingleDelete( schema, numberTracker ), distribution.get( "singleDelete" ) );
        addNumberOfTimes( queryList, new SingleInsert( schema, numberTracker ), distribution.get( "singleInsert" ) );

        long executionTime = commonExecuteWithExpectedErrors(
                queryList,
                progressReporter,
                outputDirectory,
                config.sessionCount,
                Query::getSql,
                () -> executorFactory.createExecutorInstance( csvWriter ),
                new Random(),
                expectedExceptions
        );

        analyzeAndStore( outputDirectory );
        return executionTime;
    }

    private void analyzeAndStore(File outputDirectory) {
        Properties analysis = new Properties() {
            @Override
            public synchronized void store( OutputStream out, String comments) throws IOException {
                Properties stringOnly = new Properties();
                for (Map.Entry<Object, Object> e : this.entrySet()) {
                    stringOnly.setProperty(e.getKey().toString(), e.getValue().toString());
                }
                stringOnly.store(out, comments);
            }
        };

        String fileName = String.format(
                "analysis_S%d_N%d_E%d_R%f.txt",
                config.sessionCount,
                config.namespaceCount,
                config.entityCount,
                config.readWriteRatio);

        File analysisFile = new File(outputDirectory, fileName);
        analyze(analysis, analysisFile);

        analysis.put( "numberOfSessions", config.sessionCount );
        analysis.put( "numberOfNamespaces", config.namespaceCount );
        analysis.put("numberOfEntitiesPerNamespace", config.entityCount );
        analysis.put( "readWriteRatio", config.readWriteRatio );

        try (FileOutputStream out = new FileOutputStream(analysisFile)) {
            analysis.store(out, "Analysis results");
        } catch (IOException e) {
            log.error("Exception while generating analysis", e);
        }
    }


    private void addNumberOfTimes( List<QueryListEntry> list, QueryBuilder queryBuilder, int numberOfTimes ) {
        int id = queryTypes.size() + 1;
        queryTypes.put( id, queryBuilder.getNewQuery().getSql() );
        measuredTimePerQueryType.put( id, Collections.synchronizedList( new LinkedList<>() ) );
        for ( int i = 0; i < numberOfTimes; i++ ) {
            list.add( new QueryListEntry( queryBuilder.getNewQuery(), id ) );
        }
    }


    public static Map<String, Integer> distributeQueries( int totalQueryCount, double readWriteRatio ) {
        int numberOfReads = (int) Math.round( totalQueryCount * readWriteRatio );
        int numberOfWrites = totalQueryCount - numberOfReads;

        Map<String, Integer> distribution = new HashMap<>();

        String[] readTypes = { "singleRead", "rangeRead", "fullRead" };
        int baseReadCount = numberOfReads / readTypes.length;
        int remainingReads = numberOfReads % readTypes.length;

        for ( int i = 0; i < readTypes.length; i++ ) {
            distribution.put( readTypes[i], baseReadCount + (i < remainingReads ? 1 : 0) );
        }

        String[] writeTypes = { "singleUpdate", "singleInsert", "singleDelete", "rangeUpdate" };
        int baseWriteCount = numberOfWrites / writeTypes.length;
        int remainingWrites = numberOfWrites % writeTypes.length;

        for ( int i = 0; i < writeTypes.length; i++ ) {
            distribution.put( writeTypes[i], baseWriteCount + (i < remainingWrites ? 1 : 0) );
        }

        return distribution;
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        log.info( "Warm-up..." );
        Executor executor = null;
        NumberTracker numberTracker = new NumberTracker( config, Mode.WARMUP );
        LockingBenchSchema schema = new LockingBenchSchema( config );
        for ( int i = 0; i < config.numberOfWarmUpIterations; i++ ) {
            try {
                executor = executorFactory.createExecutorInstance();

                executor.executeQuery( new SingleRead( schema, numberTracker ).getNewQuery() );
                executor.executeCommit();

                executor.executeQuery( new SingleInsert( schema, numberTracker ).getNewQuery() );
                executor.executeCommit();

                executor.executeQuery( new SingleDelete( schema, numberTracker ).getNewQuery() );
                executor.executeCommit();

                executor.executeQuery( new SingleUpdate( schema, numberTracker ).getNewQuery() );
                executor.executeCommit();

                executor.executeQuery( new RangeUpdate( schema, numberTracker ).getNewQuery() );
                executor.executeCommit();

                executor.executeQuery( new RangeRead( schema, numberTracker ).getNewQuery() );
                executor.executeCommit();

                executor.executeQuery( new FullRead( schema ).getNewQuery() );
                executor.executeCommit();

            } catch ( Exception e ) {
                log.error( "Exception while executing warmup queries", e );
            } finally {
                commitAndCloseExecutor( executor );
            }
            try {
                Thread.sleep( 10000 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }

        }
    }


    @Override
    public int getNumberOfInsertThreads() {
        return 1;
    }

}
