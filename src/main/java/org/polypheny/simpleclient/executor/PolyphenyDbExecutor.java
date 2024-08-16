/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2024 The Polypheny Project
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kong.unirest.core.HttpResponse;
import kong.unirest.core.Unirest;
import kong.unirest.core.json.JSONArray;
import kong.unirest.core.json.JSONObject;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
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
        if ( PolyphenyVersionSwitch.getInstance().useNewDeploySyntax ) {
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
            if ( PolyphenyVersionSwitch.getInstance().useNewAdapterDeployParameters ) {
                int dockerInstanceId = getDockerInstanceId();
                config = "{\"mode\":\"docker\",\"instanceId\":\"" + dockerInstanceId + "\",\"maxConnections\":\"25\"}";
            } else {
                config = "{\"port\":\"" + nextPort.getAndIncrement() + "\",\"maxConnections\":\"25\",\"password\":\"polypheny\",\"mode\":\"docker\",\"instanceId\":\"0\"}";
            }
        } else {
            name = "monetdb";
            config = "{\"database\":\"test\",\"host\":\"localhost\",\"maxConnections\":\"25\",\"password\":\"monetdb\",\"username\":\"monetdb\",\"port\":\"50000\",\"mode\":\"remote\"}";
        }
        if ( PolyphenyVersionSwitch.getInstance().useNewDeploySyntax ) {
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
            if ( PolyphenyVersionSwitch.getInstance().useNewAdapterDeployParameters ) {
                int dockerInstanceId = getDockerInstanceId();
                config = "{\"mode\":\"docker\",\"instanceId\":\"" + dockerInstanceId + "\",\"maxConnections\":\"25\"}";
            } else {
                config = "{\"port\":\"" + nextPort.getAndIncrement() + "\",\"maxConnections\":\"25\",\"password\":\"postgres\",\"mode\":\"docker\",\"instanceId\":\"0\"}";
            }
        } else {
            name = "postgres";
            config = "{\"database\":\"test\",\"host\":\"localhost\",\"maxConnections\":\"25\",\"password\":\"postgres\",\"username\":\"postgres\",\"port\":\"5432\",\"mode\":\"remote\"}";
        }
        if ( PolyphenyVersionSwitch.getInstance().useNewDeploySyntax ) {
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
        if ( PolyphenyVersionSwitch.getInstance().useNewDeploySyntax ) {
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
        if ( PolyphenyVersionSwitch.getInstance().useNewDeploySyntax ) {
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
        if ( PolyphenyVersionSwitch.getInstance().useNewDeploySyntax ) {
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
        String config;
        if ( PolyphenyVersionSwitch.getInstance().useNewAdapterDeployParameters ) {
            int dockerInstanceId = getDockerInstanceId();
            config = "{\"mode\":\"docker\",\"instanceId\":\"" + dockerInstanceId + "\",\"trxLifetimeLimit\":\"1209600\"}";
        } else {
            config = "{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"" + nextPort.getAndIncrement() + "\",\"persistent\":\"false\",\"trxLifetimeLimit\":\"1209600\"}";
        }
        if ( PolyphenyVersionSwitch.getInstance().useNewDeploySyntax ) {
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
        String config;
        if ( PolyphenyVersionSwitch.getInstance().useNewAdapterDeployParameters ) {
            int dockerInstanceId = getDockerInstanceId();
            config = "{\"mode\":\"docker\",\"instanceId\":\"" + dockerInstanceId + "\"}";
        } else {
            config = "{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"" + nextPort.getAndIncrement() + "\"}";
        }
        if ( PolyphenyVersionSwitch.getInstance().useNewDeploySyntax ) {
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


    default int getDockerInstanceId() {
        String url = "http://localhost:" + PolyphenyVersionSwitch.getInstance().uiPort + PolyphenyVersionSwitch.getInstance().dockerInstancesEndpoint;
        HttpResponse<String> response = Unirest.get( url ).asString();

        if ( response.getStatus() == 200 ) {
            JSONArray jsonArray = new JSONArray( response.getBody() );
            if ( !jsonArray.isEmpty() ) {
                JSONObject jsonObject = jsonArray.getJSONObject( 0 );
                return jsonObject.getInt( "id" );
            }
        }
        throw new RuntimeException( "Failed to fetch docker instance id" );
    }


    void setConfig( String key, String value );


    @Slf4j
    class PolyphenyDbInstance extends DatabaseInstance {

        private final PolyphenyControlConnector polyphenyControlConnector;
        private final AbstractConfig config;

        @Getter
        private final StatusGatherer statusGatherer;


        public PolyphenyDbInstance( PolyphenyControlConnector polyphenyControlConnector, ExecutorFactory executorFactory, File outputDirectory, AbstractConfig config ) {
            this.polyphenyControlConnector = polyphenyControlConnector;
            this.config = config;

            // Initialize feature switch
            PolyphenyVersionSwitch.initialize( config );

            // Stop Polypheny
            stopPolypheny( polyphenyControlConnector );

            // Update Polypheny Control settings
            configurePolyphenyControl( polyphenyControlConnector, config, config.resetCatalog );

            // Purge Polypheny folder
            polyphenyControlConnector.purgePolyphenyFolder();

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

            // Create status gather, however, do not start it
            statusGatherer = new StatusGatherer();
        }


        private void stopPolypheny( PolyphenyControlConnector polyphenyControlConnector ) {
            polyphenyControlConnector.stopPolypheny();
            try {
                TimeUnit.SECONDS.sleep( 3 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
            if ( polyphenyControlConnector.checkForAnyRunningPolyphenyInstances() > 0 ) {
                // Wait another two minutes for Polypheny to stop
                try {
                    TimeUnit.MINUTES.sleep( 2 );
                } catch ( InterruptedException e ) {
                    throw new RuntimeException( "Unexpected interrupt", e );
                }
            }
            if ( polyphenyControlConnector.checkForAnyRunningPolyphenyInstances() > 0 ) {
                throw new RuntimeException( "There are still running instances of Polypheny. Manual intervention is required!" );
                //log.error( "There are still running instances of Polypheny. Manual intervention is required!" );
                //System.exit( 1 );
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
                TimeUnit.SECONDS.sleep( 20 );
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
                if ( PolyphenyVersionSwitch.getInstance().hasIcarusRoutingSettings ) { // Old routing, to be removed
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
            //conf.put( "pcrtl.plugins.purge", "onStartup" );
            conf.put( "pcrtl.plugins.purge", "never" );
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
            statusGatherer.terminate();
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
                HttpResponse<String> response = Unirest.get( "http://" + ChronosCommand.hostname + ":" + PolyphenyVersionSwitch.getInstance().uiPort + "/product" ).asString();
                if ( response.isSuccess() && checkIfDockerIsReady() ) {
                    return true;
                }
            } catch ( Exception e ) {
                // ignore
            }
            return false;
        }


        private boolean checkIfDockerIsReady() {
            String url = "http://localhost:" + PolyphenyVersionSwitch.getInstance().uiPort + PolyphenyVersionSwitch.getInstance().dockerInstancesEndpoint;
            HttpResponse<String> response = Unirest.get( url ).asString();

            if ( response.getStatus() == 200 ) {
                JSONArray jsonArray = new JSONArray( response.getBody() );
                if ( !jsonArray.isEmpty() ) {
                    JSONObject jsonObject = jsonArray.getJSONObject( 0 );
                    return true;
                }
            }
            return false;
        }


        // to be removed
        public void setIcarusRoutingTraining( boolean b ) {
            PolyphenyDbExecutor executor = (PolyphenyDbExecutor) new PolyphenyDbJdbcExecutorFactory( ChronosCommand.hostname, false ).createExecutorInstance();
            try {
                // Disable icarus training
                if ( PolyphenyVersionSwitch.getInstance().hasIcarusRoutingSettings ) {  // Old routing -- to be removed
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


    @Slf4j
    class StatusGatherer {

        private ScheduledExecutorService statusGatheringService;
        private final List<PolyphenyStatus> statuses = new ArrayList<>();

        private final String url;


        private StatusGatherer() {
            url = "http://" + ChronosCommand.hostname + ":" + PolyphenyVersionSwitch.getInstance().uiPort + "/status/";
        }


        public PolyphenyStatus gatherOnce() {
            return new PolyphenyStatus(
                    Long.parseLong( Unirest.get( url + "memory-current" ).asString().getBody() ),
                    Integer.parseInt( Unirest.get( url + "transactions-active" ).asString().getBody() ),
                    Integer.parseInt( Unirest.get( url + "monitoring-queue" ).asString().getBody() )
            );
        }


        public PolyphenyFullStatus gatherFullOnce() {
            return new PolyphenyFullStatus(
                    Unirest.get( url + "uuid" ).asString().getBody(),
                    Unirest.get( url + "version" ).asString().getBody(),
                    Unirest.get( url + "hash" ).asString().getBody(),
                    Long.parseLong( Unirest.get( url + "memory-current" ).asString().getBody() ),
                    Integer.parseInt( Unirest.get( url + "transactions-since-restart" ).asString().getBody() ),
                    Integer.parseInt( Unirest.get( url + "transactions-active" ).asString().getBody() ),
                    Integer.parseInt( Unirest.get( url + "cache-implementation" ).asString().getBody() ),
                    Integer.parseInt( Unirest.get( url + "cache-queryplan" ).asString().getBody() ),
                    Integer.parseInt( Unirest.get( url + "cache-routingplan" ).asString().getBody() ),
                    Integer.parseInt( Unirest.get( url + "monitoring-queue" ).asString().getBody() )
            );
        }


        public void startStatusDataGathering( int intervalSeconds ) {
            if ( statusGatheringService != null && !statusGatheringService.isTerminated() ) {
                throw new RuntimeException( "Status gathering is already running!" );
            }
            log.info( "Start gather status data from Polypheny every " + intervalSeconds + " seconds." );
            Runnable statusGatherer = new Runnable() {
                @Override
                public void run() {
                    try {
                        statuses.add( gatherOnce() );
                    } catch ( Exception e ) {
                        log.error( "Unable to gather status data from Polypheny", e );
                    }
                }
            };
            statusGatheringService = Executors.newScheduledThreadPool( 1 );
            statusGatheringService.scheduleAtFixedRate( statusGatherer, 0, intervalSeconds, TimeUnit.SECONDS );
        }


        public List<PolyphenyStatus> stopGathering() {
            log.info( "Stop gathering status data from Polypheny." );
            if ( statusGatheringService != null ) {
                statusGatheringService.shutdown();
            } else {
                throw new RuntimeException( "Status gathering has never been started" );
            }
            return statuses;
        }


        // Only use to forcefully stop gathering in case the benchmark gets aborted (e.g., due to an error)
        public void terminate() {
            if ( statusGatheringService != null && !statusGatheringService.isTerminated() ) {
                statusGatheringService.shutdown();
            }
        }


        @Data
        public static class PolyphenyStatus {

            protected final long currentMemory;
            protected final int numOfActiveTrx;
            protected final int monitoringQueueSize;

        }


        @EqualsAndHashCode(callSuper = true)
        @Getter
        public static class PolyphenyFullStatus extends PolyphenyStatus {

            PolyphenyFullStatus( String uui, String version, String hash, long currentMemory, long trxCount, int numOfActiveTrx, int implementationCacheSize, int queryPlanCacheSize, int routingPlanCacheSize, int monitoringQueueSize ) {
                super( currentMemory, numOfActiveTrx, monitoringQueueSize );
                this.uuid = uui;
                this.version = version;
                this.hash = hash;
                this.trxCount = trxCount;
                this.implementationCacheSize = implementationCacheSize;
                this.queryPlanCacheSize = queryPlanCacheSize;
                this.routingPlanCacheSize = routingPlanCacheSize;
            }


            private final String uuid;
            private final String version;
            private final String hash;

            private final long trxCount;

            private final int implementationCacheSize;
            private final int queryPlanCacheSize;
            private final int routingPlanCacheSize;

        }

    }


}
