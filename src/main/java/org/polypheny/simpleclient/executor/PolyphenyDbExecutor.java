/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 The Polypheny Project
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
 *
 */

package org.polypheny.simpleclient.executor;

import com.google.gson.Gson;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.control.client.PolyphenyControlConnector;
import org.polypheny.simpleclient.cli.ChronosCommand;
import org.polypheny.simpleclient.executor.MonetdbExecutor.MonetdbInstance;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PostgresExecutor.PostgresInstance;
import org.polypheny.simpleclient.scenario.AbstractConfig;


public interface PolyphenyDbExecutor extends Executor {

    void dropStore( String name ) throws ExecutorException;


    void deployStore( String name, String clazz, String config ) throws ExecutorException;


    default void deployHsqldb() throws ExecutorException {
        deployStore(
                "hsqldb",
                "org.polypheny.db.adapter.jdbc.stores.HsqldbStore",
                "{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}" );
    }


    default void deployMonetDb( boolean deployStoresUsingDocker ) throws ExecutorException {
        String config;
        if ( deployStoresUsingDocker ) {
            config = "{\"port\":\"3303\",\"maxConnections\":\"25\",\"password\":\"polypheny\",\"mode\":\"docker\",\"instanceId\":\"0\"}";
        } else {
            config = "{\"database\":\"test\",\"host\":\"localhost\",\"maxConnections\":\"25\",\"password\":\"monetdb\",\"username\":\"monetdb\",\"port\":\"50000\",\"mode\":\"remote\"}";
        }
        deployStore(
                "monetdb",
                "org.polypheny.db.adapter.jdbc.stores.MonetdbStore",
                config );
    }


    default void deployPostgres( boolean deployStoresUsingDocker ) throws ExecutorException {
        String config;
        if ( deployStoresUsingDocker ) {
            config = "{\"port\":\"3302\",\"maxConnections\":\"25\",\"password\":\"postgres\",\"mode\":\"docker\",\"instanceId\":\"0\"}";
        } else {
            config = "{\"database\":\"test\",\"host\":\"localhost\",\"maxConnections\":\"25\",\"password\":\"postgres\",\"username\":\"postgres\",\"port\":\"5432\",\"mode\":\"remote\"}";
        }
        deployStore(
                "postgres",
                "org.polypheny.db.adapter.jdbc.stores.PostgresqlStore",
                config );
    }


    default void deployCassandra( boolean deployStoresUsingDocker ) throws ExecutorException {
        String config;
        if ( deployStoresUsingDocker ) {
            config = "{\"port\":\"9043\",\"mode\":\"docker\",\"instanceId\":\"0\"}";
        } else {
            config = "{\"mode\":\"embedded\",\"host\":\"localhost\",\"port\":\"9042\",\"keyspace\":\"cassandra\",\"username\":\"cassandra\",\"password\":\"cass\"}";
        }
        deployStore(
                "cassandra",
                "org.polypheny.db.adapter.cassandra.CassandraStore",
                config );
    }


    default void deployFileStore() throws ExecutorException {
        deployStore(
                "file",
                "org.polypheny.db.adapter.file.FileStore",
                "{\"mode\":\"embedded\"}" );
    }


    default void deployCottontail() throws ExecutorException {
        deployStore(
                "cottontail",
                "org.polypheny.db.adapter.cottontail.CottontailStore",
                "{\"type\":\"Embedded\",\"host\":\"localhost\",\"port\":\"1865\",\"database\":\"cottontail\",\"engine\":\"MAPDB\",\"mode\":\"embedded\"}" );
    }


    default void deployMongoDb( boolean deployStoresUsingDocker ) throws ExecutorException {
        String config;
        if ( deployStoresUsingDocker ) {
            config = "{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"27017\",\"persistent\":\"false\", \"trxLifetimeLimit\":\"1209600\"}";
        } else {
            throw new RuntimeException( "MongoDB can only be deployed in Docker mode" );
        }
        deployStore(
                "mongodb",
                "org.polypheny.db.adapter.mongodb.MongoStore",
                config );
    }


    void setConfig( String key, String value );


    @Slf4j
    class PolyphenyDbInstance extends DatabaseInstance {

        private final PolyphenyControlConnector polyphenyControlConnector;
        private final AbstractConfig config;


        public PolyphenyDbInstance( PolyphenyControlConnector polyphenyControlConnector, ExecutorFactory executorFactory, File outputDirectory, AbstractConfig config ) {
            this.polyphenyControlConnector = polyphenyControlConnector;
            this.config = config;

            // Update settings
            Map<String, String> conf = new HashMap<>();
            conf.put( "pcrtl.pdbms.branch", config.pdbBranch.trim() );
            conf.put( "pcrtl.ui.branch", config.puiBranch.trim() );
            conf.put( "pcrtl.java.heap", "10" );
            if ( config.buildUi ) {
                conf.put( "pcrtl.buildmode", "both" );
            } else {
                conf.put( "pcrtl.buildmode", "pdb" );
            }
            conf.put( "pcrtl.clean.mode", "branchChange" );
            String args = "";
            if ( config.resetCatalog ) {
                args += "-resetCatalog ";
            }
            if ( config.memoryCatalog ) {
                args += "-memoryCatalog ";
            }
            conf.put( "pcrtl.pdbms.args", args.trim() );
            polyphenyControlConnector.setConfig( conf );

            // Pull branch and update polypheny
            polyphenyControlConnector.updatePolypheny();

            // Start Polypheny
            polyphenyControlConnector.startPolypheny();
            // Try for 300 seconds (30 times)
            for ( int i = 0; i <= 30; i++ ) {
                if ( isReady() ) {
                    break;
                }
                try {
                    TimeUnit.SECONDS.sleep( 10 );
                } catch ( InterruptedException e ) {
                    // ignore
                }
                if ( i >= 30 ) {
                    throw new RuntimeException( "System not ready. Aborting" );
                }
            }

            // Wait another five seconds for post-startup process to finish
            try {
                TimeUnit.SECONDS.sleep( 5 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }

            // Store Polypheny version for documentation
            try {
                FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "polypheny.version" );
                fw.append( polyphenyControlConnector.getVersion() );
                fw.close();
            } catch ( IOException e ) {
                log.error( "Error while logging polypheny version", e );
            }

            // Configure data stores
            PolyphenyDbExecutor executor = (PolyphenyDbExecutor) executorFactory.createExecutorInstance();
            try {
                // Remove hsqldb store
                executor.dropStore( "hsqldb" );
                // Deploy stores
                for ( String store : config.dataStores ) {
                    switch ( store ) {
                        case "hsqldb":
                            executor.deployHsqldb();
                            break;
                        case "postgres":
                            if ( !config.deployStoresUsingDocker ) {
                                PostgresInstance.reset();
                            }
                            executor.deployPostgres( config.deployStoresUsingDocker );
                            break;
                        case "monetdb":
                            if ( !config.deployStoresUsingDocker ) {
                                MonetdbInstance.reset();
                            }
                            executor.deployMonetDb( config.deployStoresUsingDocker );
                            break;
                        case "cassandra":
                            executor.deployCassandra( config.deployStoresUsingDocker );
                            break;
                        case "file":
                            executor.deployFileStore();
                            break;
                        case "cottontail":
                            executor.deployCottontail();
                            break;
                        case "mongodb":
                            executor.deployMongoDb( config.deployStoresUsingDocker );
                            break;
                        default:
                            throw new RuntimeException( "Unknown data store: " + store );
                    }
                }
                executor.executeCommit();
            } catch ( ExecutorException e ) {
                throw new RuntimeException( "Exception while configuring stores", e );
            } finally {
                try {
                    executor.closeConnection();
                } catch ( ExecutorException e ) {
                    log.error( "Exception while closing connection", e );
                }
            }

            // Update polypheny config
            executor = (PolyphenyDbExecutor) executorFactory.createExecutorInstance();
            try {
                // disable active tracking (dynamic querying)
                executor.setConfig( "statistics/activeTracking", "false" );
                // Set router
                if ( !config.pdbBranch.equalsIgnoreCase( "universal-routing" ) ) { // Old routing, to be removed
                    switch ( config.router ) {
                        case "simple":
                            executor.setConfig( "routing/router", "org.polypheny.db.router.SimpleRouter$SimpleRouterFactory" );
                            break;
                        case "icarus":
                            executor.setConfig( "routing/router", "org.polypheny.db.router.IcarusRouter$IcarusRouterFactory" );
                            setIcarusRoutingTraining( false );
                            break;
                        default:
                            throw new RuntimeException( "Unknown configuration value for router: " + config.router );
                    }
                } else {
                    // Set Routers
                    List<String> routers = new ArrayList<>();
                    for ( String router : config.routers ) {
                        switch ( router ) {
                            case "Simple":
                                routers.add( "org.polypheny.db.routing.routers.SimpleRouter$SimpleRouterFactory" );
                                break;
                            case "Icarus":
                                routers.add( "org.polypheny.db.routing.routers.IcarusRouter$IcarusRouterFactory" );
                                break;
                            case "FullPlacement":
                                routers.add( "org.polypheny.db.routing.routers.FullPlacementQueryRouter$FullPlacementQueryRouterFactory" );
                                break;
                            default:
                                throw new RuntimeException( "Unknown configuration value for 'routers': " + router );
                        }
                    }
                    Gson gson = new Gson();
                    executor.setConfig( "routing/routers", gson.toJson( routers ) );
                    // Configure placement strategy for new tables
                    switch ( config.newTablePlacementStrategy ) {
                        case "Single":
                            executor.setConfig( "routing/createPlacementStrategy", "org.polypheny.db.routing.strategies.CreateSinglePlacementStrategy" );
                            break;
                        case "All":
                            executor.setConfig( "routing/createPlacementStrategy", "org.polypheny.db.routing.strategies.CreateAllPlacementStrategy" );
                            break;
                        default:
                            throw new RuntimeException( "Unknown configuration value for 'newTablePlacementStrategy': " + config.newTablePlacementStrategy );
                    }
                    // Configure placement strategy for new tables
                    executor.setConfig( "routing/planSelectionStrategy", config.planSelectionStrategy.toUpperCase() );
                    // Set cost ratio
                    double ratio = config.preCostRatio / 100.0;
                    executor.setConfig( "routing/preCostPostCostRatio", ratio + "" );
                    // Set post cost aggregation
                    executor.setConfig( "routing/postCostAggregationActive", config.postCostAggregation.equals( "always" ) ? "true" : "false" );
                    // Set routing cache
                    executor.setConfig( "runtime/routingPlanCaching", config.routingCache ? "true" : "false" );
                    // Set workload monitoring processing interval
                    switch ( config.workloadMonitoringProcessingInterval ) {
                        case "1s":
                            executor.setConfig( "runtime/queueProcessingInterval", "EVERY_SECOND" );
                            break;
                        case "5s":
                            executor.setConfig( "runtime/queueProcessingInterval", "EVERY_FIVE_SECONDS" );
                            break;
                        case "10s":
                            executor.setConfig( "runtime/queueProcessingInterval", "EVERY_TEN_SECONDS" );
                            break;
                        case "30s":
                            executor.setConfig( "runtime/queueProcessingInterval", "EVERY_THIRTY_SECONDS" );
                            break;
                        case "1min":
                            executor.setConfig( "runtime/queueProcessingInterval", "EVERY_MINUTE" );
                            break;
                        case "10min":
                            executor.setConfig( "runtime/queueProcessingInterval", "EVERY_TEN_MINUTES" );
                            break;
                        case "15min":
                            executor.setConfig( "runtime/queueProcessingInterval", "EVERY_FIFTEEN_MINUTES" );
                            break;
                        case "30min":
                            executor.setConfig( "runtime/queueProcessingInterval", "EVERY_THIRTY_MINUTES" );
                            break;
                        default:
                            throw new RuntimeException( "Unknown configuration value for 'workloadMonitoringProcessingInterval': " + config.workloadMonitoringProcessingInterval );
                    }
                    // Set workload monitoring processing elements per interval
                    executor.setConfig( "runtime/queueProcessingElements", config.workloadMonitoringElementsPerInterval + "" );
                }
                // Set Plan & Implementation Caching
                switch ( config.planAndImplementationCaching ) {
                    case "None":
                        executor.setConfig( "runtime/implementationCaching", "false" );
                        executor.setConfig( "runtime/queryPlanCaching", "false" );
                        break;
                    case "Plan":
                        executor.setConfig( "runtime/implementationCaching", "false" );
                        executor.setConfig( "runtime/queryPlanCaching", "true" );
                        break;
                    case "Implementation":
                        executor.setConfig( "runtime/implementationCaching", "true" );
                        executor.setConfig( "runtime/queryPlanCaching", "false" );
                        break;
                    case "Both":
                        executor.setConfig( "runtime/implementationCaching", "true" );
                        executor.setConfig( "runtime/queryPlanCaching", "true" );
                        break;
                    default:
                        throw new RuntimeException( "Unknown configuration value for planAndImplementationCaching: " + config.planAndImplementationCaching );
                }
                executor.executeCommit();
            } catch ( ExecutorException e ) {
                throw new RuntimeException( "Exception while updating polypheny config", e );
            } finally {
                try {
                    executor.closeConnection();
                } catch ( ExecutorException e ) {
                    log.error( "Exception while closing connection", e );
                }
            }

            // Wait 5 seconds to let the the config changes take effect
            try {
                TimeUnit.SECONDS.sleep( 5 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }


        @Override
        public void tearDown() {
            polyphenyControlConnector.stopPolypheny();
            try {
                TimeUnit.SECONDS.sleep( 3 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
            for ( String store : config.dataStores ) {
                switch ( store ) {
                    case "hsqldb":
                        break;
                    case "postgres":
                        if ( !config.deployStoresUsingDocker ) {
                            PostgresInstance.reset();
                        }
                        break;
                    case "monetdb":
                        if ( !config.deployStoresUsingDocker ) {
                            MonetdbInstance.reset();
                        }
                        break;
                    case "cassandra":
                        break;
                    case "file":
                        break;
                    case "cottontail":
                        break;
                    case "mongodb":
                        break;
                    default:
                        throw new RuntimeException( "Unknown data store: " + store );
                }
            }
        }


        private boolean isReady() {
            try {
                HttpResponse<String> response = Unirest.get( "http://" + ChronosCommand.hostname + ":8080/getTypeInfo" ).asString();
                if ( response.isSuccess() ) {
                    return true;
                }
            } catch ( Exception e ) {
                // ignore
            }
            return false;
        }


        // to be removed
        public void setIcarusRoutingTraining( boolean b ) {
            PolyphenyDbExecutor executor = (PolyphenyDbExecutor) new PolyphenyDbJdbcExecutorFactory( ChronosCommand.hostname, false ).createExecutorInstance();
            try {
                // Disable icarus training
                if ( !config.pdbBranch.equalsIgnoreCase( "universal-routing" ) ) {
                    executor.setConfig( "icarusRouting/training", b ? "true" : "false" );
                    executor.executeCommit();
                } else {
                    executor.setConfig( "routing/postCostAggregationActive", b ? "true" : "false" );
                    executor.executeCommit();
                }
            } catch ( ExecutorException e ) {
                throw new RuntimeException( "Exception while updating polypheny config", e );
            } finally {
                try {
                    executor.closeConnection();
                } catch ( ExecutorException e ) {
                    log.error( "Exception while closing connection", e );
                }
            }
        }


        public void setPostCostAggregation( boolean b ) {
            PolyphenyDbExecutor executor = (PolyphenyDbExecutor) new PolyphenyDbJdbcExecutorFactory( ChronosCommand.hostname, false ).createExecutorInstance();
            try {
                executor.setConfig( "routing/postCostAggregationActive", b ? "true" : "false" );
                executor.executeCommit();
            } catch ( ExecutorException e ) {
                throw new RuntimeException( "Exception while updating polypheny config", e );
            } finally {
                try {
                    executor.closeConnection();
                } catch ( ExecutorException e ) {
                    log.error( "Exception while closing connection", e );
                }
            }
        }

    }

}
