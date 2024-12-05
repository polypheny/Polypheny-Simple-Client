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

package org.polypheny.simpleclient.scenario.oltpbench.ycsb;

import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor.PolyphenyDbInstance;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchScenario;


@Slf4j
public class Ycsb extends AbstractOltpBenchScenario {

    public Ycsb( ExecutorFactory executorFactory, YcsbConfig config, boolean dumpQueryList, QueryMode queryMode ) {
        super( executorFactory, config, dumpQueryList, queryMode );
    }


    @Override
    protected void preSchemaCreationTasks( DatabaseInstance databaseInstance, ExecutorFactory executorFactory ) {
        Executor executor = executorFactory.createExecutorInstance();
        try {
            // Set table placement strategy
            if ( databaseInstance instanceof PolyphenyDbInstance && ((YcsbConfig) config).partitionTable ) {
                if ( config.dataStores.size() > 1 ) {
                    ((PolyphenyDbExecutor) executor).setConfig( "routing/createPlacementStrategy", "org.polypheny.db.routing.strategies.CreateAllPlacementStrategy" );
                }
            }
        } finally {
            commitAndCloseExecutor( executor );
        }
    }


    @Override
    protected void postSchemaCreationTasks( DatabaseInstance databaseInstance, ExecutorFactory executorFactory ) throws ExecutorException {
        Executor executor = executorFactory.createExecutorInstance();
        try {
            // Partition usertable table
            if ( databaseInstance instanceof PolyphenyDbInstance && ((YcsbConfig) config).partitionTable ) {
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


    @Override
    protected void postDataGenerationTasks( DatabaseInstance databaseInstance, ExecutorFactory executorFactory ) throws ExecutorException {
        Executor executor = executorFactory.createExecutorInstance();
        try {
            // Partition usertable
            if ( databaseInstance instanceof PolyphenyDbInstance && ((YcsbConfig) config).partitionTable ) {
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
