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

package org.polypheny.simpleclient.scenario.ycsb;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor.PolyphenyDbInstance;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.Scenario;


@Slf4j
public class YcsbExternal extends Scenario {

    private final YcsbConfig config;
    private final YcsbRunner runner;
    public static final String DOCUMENT_NAMESPACE = "ycsb";

    public YcsbExternal(ExecutorFactory executorFactory, YcsbConfig config, YcsbRunner runner, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, true, dumpQueryList, queryMode );
        this.config = config;
        this.runner = runner;
    }

    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        if ( queryMode != QueryMode.TABLE ) {
            throw new UnsupportedOperationException( "Unsupported query mode: " + queryMode.name() );
        }
        if ( "polypheny-mongoql".equals( config.system ) ) {
            createDocumentSchema();
            return;
        }

        try {
            preSchemaCreationTasks( databaseInstance, executorFactory );

            Executor executor = executorFactory.createExecutorInstance();
            try {
                StringBuilder sql = new StringBuilder( "CREATE TABLE USERTABLE (YCSB_KEY VARCHAR(255) NOT NULL" );

                for ( int i = 0; i < config.fieldCount; i++ ) {
                    sql.append( ", FIELD" )
                            .append( i )
                            .append( " VARCHAR(" )
                            .append( config.fieldLength )
                            .append( ")" );
                }

                sql.append( ", PRIMARY KEY (YCSB_KEY))" );

                executor.executeQuery( RawQuery.builder()
                        .sql( sql.toString() )
                        .expectResultSet( false )
                        .build() );            } finally {
                commitAndCloseExecutor( executor );
            }

            postSchemaCreationTasks( databaseInstance, executorFactory );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating YCSB schema", e );
        }
    }

    private void createDocumentSchema() {
        Executor executor = executorFactory.createExecutorInstance(
                null, DOCUMENT_NAMESPACE
        );

        try {
            executor.executeQuery( RawQuery.builder()
                    .mongoQl( "use " + DOCUMENT_NAMESPACE )
                    .expectResultSet( false )
                    .build() );

            executor.executeQuery( RawQuery.builder()
                    .mongoQl( "db.createCollection(usertable)" )
                    .expectResultSet( false )
                    .build() );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while creating YCSB document schema", e );
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        if ( queryMode != QueryMode.TABLE ) {
            throw new UnsupportedOperationException( "Unsupported query mode: " + queryMode.name() );
        }

        File logFile = new File( "results/ycsb-load.log" );

        log.info( "Loading {} records using YCSB", config.recordCount );

        runner.load( logFile );

        try {
            postDataGenerationTasks( databaseInstance, executorFactory );
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception after loading YCSB data", e );
        }

        log.info( "YCSB load process finished. See {}", logFile.getAbsolutePath() );
    }

    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        File logFile = new File( outputDirectory, "ycsb-run.log" );

        log.info( "Executing {} YCSB operations", config.operationCount );

        long startTime = System.nanoTime();
        runner.run( logFile );
        long runtime = System.nanoTime() - startTime;

        log.info( "YCSB run process finished. See {}", logFile.getAbsolutePath() );

        return runtime;
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        // TODO: warmUp
    }


    @Override
    public void analyze( Properties properties, File outputDirectory ) {
        // TODO: Extract benchmark metrics from the YCSB output.
    }


    @Override
    public int getNumberOfInsertThreads() {
        return config.numberOfThreads;
    }


    protected void preSchemaCreationTasks( DatabaseInstance databaseInstance, ExecutorFactory executorFactory ) {

        Executor executor = executorFactory.createExecutorInstance();
        try {
            // Set table placement strategy
            if ( databaseInstance instanceof PolyphenyDbInstance && config.partitionTable ) {
                if ( config.dataStores.size() > 1 ) {
                    ((PolyphenyDbExecutor) executor).setConfig( "routing/createPlacementStrategy", "org.polypheny.db.routing.strategies.CreateAllPlacementStrategy" );
                }
            }
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    protected void postSchemaCreationTasks( DatabaseInstance databaseInstance, ExecutorFactory executorFactory ) throws ExecutorException {
        Executor executor = executorFactory.createExecutorInstance();
        try {
            // Partition usertable table
            if ( databaseInstance instanceof PolyphenyDbInstance && config.partitionTable ) {
                if ( config.dataStores.size() > 1 ) {
                    List<String> storeNames = ((PolyphenyDbExecutor) executor).storeNames;
                    String partitionNames = storeNames.stream()
                            .map( obj -> "p_" + obj )
                            .collect( Collectors.joining( "," ) );
                    executor.executeQuery( RawQuery.builder()
                            .sql( "ALTER TABLE usertable PARTITION BY HASH (YCSB_KEY) WITH (" + partitionNames + ")" )
                            .expectResultSet( false )
                            .build() );
                }
            }
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    protected void postDataGenerationTasks( DatabaseInstance databaseInstance, ExecutorFactory executorFactory ) throws ExecutorException {
        Executor executor = executorFactory.createExecutorInstance();
        try {
            // Partition usertable
            if ( databaseInstance instanceof PolyphenyDbInstance && config.partitionTable ) {
                if ( config.dataStores.size() > 1 ) {
                    List<String> storeNames = ((PolyphenyDbExecutor) executor).storeNames;
                    for ( String storeName : storeNames ) {
                        executor.executeQuery( RawQuery.builder()
                                .sql( "ALTER TABLE usertable MODIFY PARTITIONS (p_" + storeName + ") ON STORE " + storeName )
                                .expectResultSet( false )
                                .build() );
                    }
                }
            }
        } finally {
            commitAndCloseExecutor( executor );
        }
    }

}
