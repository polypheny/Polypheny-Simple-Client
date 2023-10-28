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
import java.util.concurrent.atomic.AtomicInteger;
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

    AtomicInteger storeCounter = new AtomicInteger();
    AtomicInteger nextPort = new AtomicInteger( 3300 );

    List<String> storeNames = new ArrayList<>();


    void dropStore( String name ) throws ExecutorException;


    void deployStore( String name, String clazz, String config ) throws ExecutorException;


    void deployAdapter( String name, String adapterIdentifier, String type, String config ) throws ExecutorException;


    default String deployHsqldb() throws ExecutorException {
        String storeName = "hsqldb" + storeCounter.getAndIncrement();
        String config = "{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}";
        if ( useNewDeploySyntax() ) {
            deployAdapter(
                    storeName,
                    "HSQLDB",
                    "STORE",
                    config
            );
        } else {
            deployStore(
                    storeName,
                    "org.polypheny.db.adapter.jdbc.stores.HsqldbStore",
                    config );
        }
        storeNames.add( storeName );
        return storeName;
    }


    default String deployMonetDb( boolean deployStoresUsingDocker ) throws ExecutorException {
        String config;
        String name;
        if ( deployStoresUsingDocker ) {
            name = "monetdb" + storeCounter.getAndIncrement();
            config = "{\"port\":\"" + nextPort.getAndIncrement() + "\",\"maxConnections\":\"25\",\"password\":\"polypheny\",\"mode\":\"docker\",\"instanceId\":\"0\"}";
        } else {
            name = "monetdb";
            config = "{\"database\":\"test\",\"host\":\"localhost\",\"maxConnections\":\"25\",\"password\":\"monetdb\",\"username\":\"monetdb\",\"port\":\"50000\",\"mode\":\"remote\"}";
        }
        if ( useNewDeploySyntax() ) {
            deployAdapter(
                    name,
                    "MONETDB",
                    "STORE",
                    config );
        } else {
            deployStore(
                    name,
                    "org.polypheny.db.adapter.jdbc.stores.MonetdbStore",
                    config );
        }
        storeNames.add( name );
        return name;
    }


    default String deployPostgres( boolean deployStoresUsingDocker ) throws ExecutorException {
        String config;
        String name;
        if ( deployStoresUsingDocker ) {
            name = "postgres" + storeCounter.getAndIncrement();
            config = "{\"port\":\"" + nextPort.getAndIncrement() + "\",\"maxConnections\":\"25\",\"password\":\"postgres\",\"mode\":\"docker\",\"instanceId\":\"0\"}";
        } else {
            name = "postgres";
            config = "{\"database\":\"test\",\"host\":\"localhost\",\"maxConnections\":\"25\",\"password\":\"postgres\",\"username\":\"postgres\",\"port\":\"5432\",\"mode\":\"remote\"}";
        }
        if ( useNewDeploySyntax() ) {
            deployAdapter(
                    name,
                    "POSTGRESQL",
                    "STORE",
                    config );
        } else {
            deployStore(
                    name,
                    "org.polypheny.db.adapter.jdbc.stores.PostgresqlStore",
                    config );
        }
        storeNames.add( name );
        return name;
    }


    default String deployCassandra( boolean deployStoresUsingDocker ) throws ExecutorException {
        String config;
        String name;
        if ( deployStoresUsingDocker ) {
            name = "cassandra" + storeCounter.getAndIncrement();
            config = "{\"port\":\"" + nextPort.getAndIncrement() + "\",\"mode\":\"docker\",\"instanceId\":\"0\"}";
        } else {
            name = "cassandra";
            config = "{\"mode\":\"embedded\",\"host\":\"localhost\",\"port\":\"9042\",\"keyspace\":\"cassandra\",\"username\":\"cassandra\",\"password\":\"cass\"}";
        }
        if ( useNewDeploySyntax() ) {
            deployAdapter(
                    name,
                    "CASSANDRA",
                    "STORE",
                    config );
        } else {
            deployStore(
                    name,
                    "org.polypheny.db.adapter.cassandra.CassandraStore",
                    config );
        }
        storeNames.add( name );
        return name;
    }


    default String deployFileStore() throws ExecutorException {
        String storeName = "file" + storeCounter.getAndIncrement();
        String config = "{\"mode\":\"embedded\"}";
        if ( useNewDeploySyntax() ) {
            deployAdapter(
                    storeName,
                    "FILE",
                    "STORE",
                    config );
        } else {
            deployStore(
                    storeName,
                    "org.polypheny.db.adapter.file.FileStore",
                    config );
        }
        storeNames.add( storeName );
        return storeName;
    }


    default String deployCottontail() throws ExecutorException {
        String storeName = "cottontail" + storeCounter.getAndIncrement();
        String config = "{\"type\":\"Embedded\",\"host\":\"localhost\",\"port\":\"" + nextPort.getAndIncrement() + "\",\"database\":\"cottontail\",\"engine\":\"MAPDB\",\"mode\":\"embedded\"}";
        if ( useNewDeploySyntax() ) {
            deployAdapter(
                    storeName,
                    "COTTONTAIL",
                    "STORE",
                    config );
        } else {
            deployStore(
                    storeName,
                    "org.polypheny.db.adapter.cottontail.CottontailStore",
                    config );
        }
        storeNames.add( storeName );
        return storeName;
    }


    default String deployMongoDb() throws ExecutorException {
        String storeName = "mongodb" + storeCounter.getAndIncrement();
        String config = "{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"" + nextPort.getAndIncrement() + "\",\"persistent\":\"false\",\"trxLifetimeLimit\":\"1209600\"}";
        if ( useNewDeploySyntax() ) {
            deployAdapter(
                    storeName,
                    "MONGODB",
                    "STORE",
                    config );
        } else {
            deployStore(
                    storeName,
                    "org.polypheny.db.adapter.mongodb.MongoStore",
                    config );
        }
        storeNames.add( storeName );
        return storeName;
    }

    default String deployNeo4j() throws ExecutorException {
        String storeName = "neo4j" + storeCounter.getAndIncrement();
        String config = "{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"" + nextPort.getAndIncrement() + "\"}";
        if ( useNewDeploySyntax() ) {
            deployAdapter(
                    storeName,
                    "NEO4J",
                    "STORE",
                    config );
        } else {
            deployStore(
                    storeName,
                    "org.polypheny.db.adapter.neo4j.Neo4jStore",
                    config );
        }
        storeNames.add( storeName );
        return storeName;
    }


    void setConfig( String key, String value );


    @Slf4j
    class PolyphenyDbInstance extends DatabaseInstance {

        private final PolyphenyControlConnector polyphenyControlConnector;
        private final AbstractConfig config;


        public PolyphenyDbInstance( PolyphenyControlConnector polyphenyControlConnector, ExecutorFactory executorFactory, File outputDirectory, AbstractConfig config ) {
            this.polyphenyControlConnector = polyphenyControlConnector;
            this.config = config;

            // Stop Polypheny
            stopPolypheny( polyphenyControlConnector );

            // Update Polypheny Control settings
            configurePolyphenyControl( polyphenyControlConnector, config, config.resetCatalog );

            // Pull branch and update polypheny
            polyphenyControlConnector.updatePolypheny();

            // Start Polypheny
            startPolypheny( polyphenyControlConnector );

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
            executor.setNewDeploySyntax( !config.pdbBranch.equalsIgnoreCase( "old-routing" ) );
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
                            executor.deployMongoDb();
                            break;
                        case "neo4j":
                            executor.deployNeo4j();
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

            pushConfiguration( executorFactory, config );

            // Wait 5 seconds to let the config changes take effect
            try {
                TimeUnit.SECONDS.sleep( 5 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }


        private void stopPolypheny( PolyphenyControlConnector polyphenyControlConnector ) {
            polyphenyControlConnector.stopPolypheny();
            try {
                TimeUnit.SECONDS.sleep( 3 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }


        private void startPolypheny( PolyphenyControlConnector polyphenyControlConnector ) {
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
        }


        public void restartPolypheny() {
            // Stop Polypheny
            stopPolypheny( polyphenyControlConnector );

            // Update Polypheny Control settings
            configurePolyphenyControl( polyphenyControlConnector, config, false );

            // Start Polypheny
            startPolypheny( polyphenyControlConnector );

            // Wait a minute for statistics to be gathered
            try {
                TimeUnit.SECONDS.sleep( 60 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        }


        protected void pushConfiguration( ExecutorFactory executorFactory, AbstractConfig config ) {
            PolyphenyDbExecutor executor;
            // Update polypheny config
            executor = (PolyphenyDbExecutor) executorFactory.createExecutorInstance();
            try {
                // Disable statistics (active tracking)
                executor.setConfig( "statistics/activeTracking", "false" );
                // Set router
                if ( config.pdbBranch.equalsIgnoreCase( "old-routing" ) ) { // Old routing, to be removed
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
                        case "Optimized":
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
        }


        protected void configurePolyphenyControl( PolyphenyControlConnector polyphenyControlConnector, AbstractConfig config, boolean resetCatalog ) {
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
            if ( resetCatalog ) {
                args += "-resetCatalog ";
            }
            if ( config.memoryCatalog ) {
                args += "-memoryCatalog ";
            }
            conf.put( "pcrtl.pdbms.args", args.trim() );
            polyphenyControlConnector.setConfig( conf );
        }


        @Override
        public void tearDown() {
            stopPolypheny( polyphenyControlConnector );
            for ( String store : config.dataStores ) {
                tearDownStore( store );
            }
        }


        private void tearDownStore( String store ) {
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
                case "neo4j":
                    break;
                default:
                    throw new RuntimeException( "Unknown data store: " + store );
            }
        }


        private boolean isReady() {
            try {
                HttpResponse<String> response = Unirest.get( "http://" + ChronosCommand.hostname + ":" + ChronosCommand.uiPort + "/product" ).asString();
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
                if ( config.pdbBranch.equalsIgnoreCase( "old-routing" ) ) {  // Old routing -- to be removed
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


        public void setWorkloadMonitoring( boolean b ) {
            PolyphenyDbExecutor executor = (PolyphenyDbExecutor) new PolyphenyDbJdbcExecutorFactory( ChronosCommand.hostname, false ).createExecutorInstance();
            try {
                executor.setConfig( "runtime/monitoringQueueActive", b ? "true" : "false" );
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


    void setNewDeploySyntax( boolean useNewDeploySyntax );

    boolean useNewDeploySyntax();

}
