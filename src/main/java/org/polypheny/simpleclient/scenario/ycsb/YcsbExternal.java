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
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.main.ProgressReporter;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.Scenario;


@Slf4j
public class YcsbExternal extends Scenario {

    public static final String DOCUMENT_NAMESPACE = "ycsb";

    private final YcsbConfig config;
    private final YcsbRunner runner;
    // Only schema creation needs the name returned by adapter deployment.
    private final String adapterName;


    public YcsbExternal( ExecutorFactory executorFactory, YcsbConfig config, YcsbRunner runner, String adapterName ) {
        super( executorFactory, true, false, QueryMode.TABLE );
        this.config = config;
        this.runner = runner;
        this.adapterName = adapterName;
    }


    @Override
    public void createSchema( DatabaseInstance databaseInstance, boolean includingKeys ) {
        if ( adapterName == null || !adapterName.matches( "[a-z][a-z0-9_]*" ) ) {
            throw new IllegalArgumentException( "Schema creation requires a deployed adapter name" );
        }

        Executor executor = "mongoql".equals( config.accessMode )
                ? executorFactory.createExecutorInstance( null, DOCUMENT_NAMESPACE )
                : executorFactory.createExecutorInstance();
        try {
            if ( "mongoql".equals( config.accessMode ) ) {
                executor.executeQuery( RawQuery.builder().mongoQl( "use " + DOCUMENT_NAMESPACE ).expectResultSet( false ).build() );
                executor.executeQuery( RawQuery.builder()
                        .mongoQl( "db.createCollection(usertable).store(" + adapterName + ")" )
                        .expectResultSet( false ).build() );
            } else {
                executor.executeQuery( RawQuery.builder().sql( createTableSql() ).expectResultSet( false ).build() );
            }
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            if ( "sql".equals( config.accessMode ) ) {
                try {
                    executor.executeRollback();
                } catch ( ExecutorException rollbackError ) {
                    e.addSuppressed( rollbackError );
                }
            }
            throw new IllegalStateException( "Could not create YCSB schema on " + adapterName, e );
        } finally {
            try {
                executor.closeConnection();
            } catch ( ExecutorException e ) {
                log.warn( "Could not close schema connection", e );
            }
        }
    }


    private String createTableSql() {
        StringBuilder sql = new StringBuilder( "CREATE TABLE USERTABLE (YCSB_KEY VARCHAR(255) NOT NULL" );
        for ( int i = 0; i < config.fieldCount; i++ ) {
            sql.append( ", FIELD" ).append( i ).append( " VARCHAR(" ).append( config.fieldLength ).append( ")" );
        }
        sql.append( ", PRIMARY KEY (YCSB_KEY)) ON STORE \"" ).append( adapterName ).append( "\"" );
        return sql.toString();
    }


    @Override
    public void generateData( DatabaseInstance databaseInstance, ProgressReporter progressReporter ) {
        File logFile = new File( "results/ycsb-load.log" );
        log.info( "Loading {} records using YCSB", config.recordCount );
        runner.load( logFile );
        log.info( "YCSB load completed. See {}", logFile.getAbsolutePath() );
    }


    @Override
    public long execute( ProgressReporter progressReporter, CsvWriter csvWriter, File outputDirectory, int numberOfThreads ) {
        File logFile = new File( outputDirectory, "ycsb-run.log" );
        log.info( "Executing {} YCSB operations", config.operationCount );
        long startTime = System.nanoTime();
        runner.run( logFile );
        log.info( "YCSB workload completed. See {}", logFile.getAbsolutePath() );
        return System.nanoTime() - startTime;
    }


    @Override
    public void warmUp( ProgressReporter progressReporter ) {
        throw new UnsupportedOperationException( "External YCSB warmup is not implemented yet" );
    }


    @Override
    public void analyze( Properties properties, File outputDirectory ) {
        // The runner checks operation status. Metric export can be added later.
    }


    @Override
    public int getNumberOfInsertThreads() {
        return config.numberOfThreads;
    }
}
