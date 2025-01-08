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

package org.polypheny.simpleclient.main;

import ch.unibas.dmi.dbis.chronos.agent.AbstractChronosAgent;
import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient.ChronosLogHandler;
import ch.unibas.dmi.dbis.chronos.agent.ChronosJob;
import ch.unibas.dmi.dbis.chronos.agent.ExecutionException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.control.client.ClientData;
import org.polypheny.control.client.ClientType;
import org.polypheny.control.client.LogHandler;
import org.polypheny.control.client.PolyphenyControlConnector;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.cli.ChronosCommand;
import org.polypheny.simpleclient.executor.CottontaildbExecutor.CottontailExecutorFactory;
import org.polypheny.simpleclient.executor.CottontaildbExecutor.CottontailInstance;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.DatabaseInstance;
import org.polypheny.simpleclient.executor.MonetdbExecutor.MonetdbExecutorFactory;
import org.polypheny.simpleclient.executor.MonetdbExecutor.MonetdbInstance;
import org.polypheny.simpleclient.executor.OltpBenchPolyphenyDbExecutor.OltpBenchPolyphenyDbExecutorFactory;
import org.polypheny.simpleclient.executor.OltpBenchPolyphenyDbExecutor.OltpBenchPolyphenyInstance;
import org.polypheny.simpleclient.executor.OltpBenchPostgresExecutor.OltpBenchPostgresExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbCypherExecutor.PolyphenyDbCypherExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor.PolyphenyDbInstance;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor.StatusGatherer;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor.StatusGatherer.PolyphenyFullStatus;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor.StatusGatherer.PolyphenyStatus;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbMongoQlExecutor.PolyphenyDbMongoQlExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbMultiExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbRestExecutor.PolyphenyDbRestExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyVersionSwitch;
import org.polypheny.simpleclient.executor.PostgresExecutor.PostgresExecutorFactory;
import org.polypheny.simpleclient.executor.PostgresExecutor.PostgresInstance;
import org.polypheny.simpleclient.executor.SurrealDBExecutor.SurrealDBExecutorFactory;
import org.polypheny.simpleclient.executor.SurrealDBExecutor.SurrealDbInstance;
import org.polypheny.simpleclient.scenario.AbstractConfig;
import org.polypheny.simpleclient.scenario.Scenario;
import org.polypheny.simpleclient.scenario.coms.Coms;
import org.polypheny.simpleclient.scenario.coms.ComsConfig;
import org.polypheny.simpleclient.scenario.docbench.DocBench;
import org.polypheny.simpleclient.scenario.docbench.DocBenchConfig;
import org.polypheny.simpleclient.scenario.gavel.Gavel;
import org.polypheny.simpleclient.scenario.gavel.GavelConfig;
import org.polypheny.simpleclient.scenario.graph.GraphBench;
import org.polypheny.simpleclient.scenario.graph.GraphBenchConfig;
import org.polypheny.simpleclient.scenario.knnbench.KnnBench;
import org.polypheny.simpleclient.scenario.knnbench.KnnBenchConfig;
import org.polypheny.simpleclient.scenario.multibench.MultiBench;
import org.polypheny.simpleclient.scenario.multibench.MultiBenchConfig;
import org.polypheny.simpleclient.scenario.multimedia.MultimediaBench;
import org.polypheny.simpleclient.scenario.multimedia.MultimediaConfig;
import org.polypheny.simpleclient.scenario.oltpbench.auctionmark.AuctionMark;
import org.polypheny.simpleclient.scenario.oltpbench.auctionmark.AuctionMarkConfig;
import org.polypheny.simpleclient.scenario.oltpbench.smallbank.SmallBank;
import org.polypheny.simpleclient.scenario.oltpbench.smallbank.SmallBankConfig;
import org.polypheny.simpleclient.scenario.oltpbench.tpcc.Tpcc;
import org.polypheny.simpleclient.scenario.oltpbench.tpcc.TpccConfig;
import org.polypheny.simpleclient.scenario.oltpbench.tpch.Tpch;
import org.polypheny.simpleclient.scenario.oltpbench.tpch.TpchConfig;
import org.polypheny.simpleclient.scenario.oltpbench.ycsb.Ycsb;
import org.polypheny.simpleclient.scenario.oltpbench.ycsb.YcsbConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Slf4j
public class ChronosAgent extends AbstractChronosAgent {

    // Whether the individual execution time of every query should be stored in the result json
    // This massively increases the amount of data stored in the Chronos Control database
    public static final boolean STORE_INDIVIDUAL_QUERY_TIMES = false;

    public final String[] supports;
    private PolyphenyControlConnector polyphenyControlConnector = null;

    private final boolean writeCsv;
    private final boolean dumpQueryList;
    private QueryMode queryMode;

    private String dockerContainerName = null;


    public ChronosAgent( InetAddress address, int port, boolean secure, boolean useHostname, String environment, String[] supports, boolean writeCsv, boolean dumpQueryList ) {
        super( address, port, secure, useHostname, environment );
        this.writeCsv = writeCsv;
        this.dumpQueryList = dumpQueryList;
        this.supports = supports;
        try {
            LogHandler logHandler = new LogHandler() {
                private final Logger CONTROL_MESSAGES_LOGGER = LoggerFactory.getLogger( "CONTROL_MESSAGES_LOGGER" );


                @Override
                public void handleLogMessage( String message ) {
                    CONTROL_MESSAGES_LOGGER.info( message );
                }


                @Override
                public void handleStartupMessage( String message ) {
                    CONTROL_MESSAGES_LOGGER.info( message );
                }


                @Override
                public void handleShutdownMessage( String message ) {
                    CONTROL_MESSAGES_LOGGER.info( message );
                }


                @Override
                public void handleRestartMessage( String message ) {
                    CONTROL_MESSAGES_LOGGER.info( message );
                }


                @Override
                public void handleUpdateMessage( String message ) {
                    CONTROL_MESSAGES_LOGGER.info( message );
                }

            };

            ClientData clientData = new ClientData( ClientType.BENCHMARKER, ChronosCommand.controlUsername, ChronosCommand.controlPassword );
            polyphenyControlConnector = new PolyphenyControlConnector( ChronosCommand.hostname + ":8070", clientData, logHandler );
        } catch ( URISyntaxException e ) {
            log.error( "Exception while connecting to Polypheny Control", e );
        }
    }


    @Override
    protected String[] getSupportedSystemNames() {
        return supports;
    }


    @Override
    protected Object prepare( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        // Parse CDL
        Map<String, String> parsedConfig = parseConfig( chronosJob );

        switch ( parsedConfig.get( "queryMode" ) ) {
            case "Table":
                queryMode = QueryMode.TABLE;
                break;
            case "View":
                queryMode = QueryMode.VIEW;
                break;
            case "Materialized":
                queryMode = QueryMode.MATERIALIZED;
                break;
            default:
                throw new UnsupportedOperationException( "Unknown query mode: " + queryMode.name() );
        }

        // Create Executor Factory
        Executor.ExecutorFactory executorFactory;
        switch ( parsedConfig.get( "store" ) ) {
            case "polypheny":
                executorFactory = new PolyphenyDbMultiExecutorFactory( ChronosCommand.hostname );
                break;
            case "polypheny-jdbc":
                executorFactory = new PolyphenyDbJdbcExecutorFactory( ChronosCommand.hostname, Boolean.parseBoolean( parsedConfig.get( "prepareStatements" ) ) );
                break;
            case "polypheny-rest":
                executorFactory = new PolyphenyDbRestExecutorFactory( ChronosCommand.hostname );
                break;
            case "polypheny-mongoql":
                executorFactory = new PolyphenyDbMongoQlExecutorFactory( ChronosCommand.hostname );
                break;
            case "polypheny-cypher":
                executorFactory = new PolyphenyDbCypherExecutorFactory( ChronosCommand.hostname );
                break;
            case "surrealdb":
                executorFactory = new SurrealDBExecutorFactory( ChronosCommand.hostname, "8989", true );
                break;
            case "postgres":
                dockerContainerName = DockerLauncher.launch( "postgres", "polypheny/postgres:latest", Map.of( "POSTGRES_PASSWORD", "postgres" ), List.of( 5432 ), () -> PostgresInstance.tryConnect( ChronosCommand.hostname ) );
                executorFactory = new PostgresExecutorFactory( ChronosCommand.hostname, Boolean.parseBoolean( parsedConfig.get( "prepareStatements" ) ) );
                break;
            case "monetdb":
                executorFactory = new MonetdbExecutorFactory( ChronosCommand.hostname, Boolean.parseBoolean( parsedConfig.get( "prepareStatements" ) ) );
                break;
            case "cottontail":
                executorFactory = new CottontailExecutorFactory( ChronosCommand.hostname );
                break;
            case "oltpbench-polypheny":
                executorFactory = new OltpBenchPolyphenyDbExecutorFactory( ChronosCommand.hostname );
                break;
            case "oltpbench-postgres":
                dockerContainerName = DockerLauncher.launch( "oltpbench-postgres", "polypheny/postgres:latest", Map.of( "POSTGRES_PASSWORD", "postgres" ), List.of( 5432 ), () -> PostgresInstance.tryConnect( ChronosCommand.hostname ) );
                executorFactory = new OltpBenchPostgresExecutorFactory( ChronosCommand.hostname );
                break;
            default:
                throw new RuntimeException( "Unknown system: " + parsedConfig.get( "store" ) );
        }

        Scenario scenario;
        AbstractConfig config;
        switch ( parsedConfig.get( "scenario" ) ) {
            case "gavel":
                config = new GavelConfig( parsedConfig );
                scenario = new Gavel( executorFactory, (GavelConfig) config, true, dumpQueryList, queryMode );
                break;
            case "coms":
                config = new ComsConfig( parsedConfig );
                scenario = new Coms( executorFactory, (ComsConfig) config, true, dumpQueryList, queryMode, -1 );
                break;
            case "knnBench":
                config = new KnnBenchConfig( parsedConfig );
                scenario = new KnnBench( executorFactory, (KnnBenchConfig) config, true, dumpQueryList );
                break;
            case "multimedia":
                config = new MultimediaConfig( parsedConfig );
                scenario = new MultimediaBench( executorFactory, (MultimediaConfig) config, true, dumpQueryList );
                break;
            case "graph":
                config = new GraphBenchConfig( parsedConfig );
                scenario = new GraphBench( executorFactory, (GraphBenchConfig) config, true, dumpQueryList );
                break;
            case "docbench":
                config = new DocBenchConfig( parsedConfig );
                scenario = new DocBench( executorFactory, (DocBenchConfig) config, true, dumpQueryList );
                break;
            case "multibench":
                config = new MultiBenchConfig( parsedConfig );
                scenario = new MultiBench( executorFactory, (MultiBenchConfig) config, true, dumpQueryList );
                break;
            case "auctionmark":
                config = new AuctionMarkConfig( parsedConfig );
                scenario = new AuctionMark( executorFactory, (AuctionMarkConfig) config, dumpQueryList, queryMode );
                break;
            case "smallbank":
                config = new SmallBankConfig( parsedConfig );
                scenario = new SmallBank( executorFactory, (SmallBankConfig) config, dumpQueryList, queryMode );
                break;
            case "tpcc":
                config = new TpccConfig( parsedConfig );
                scenario = new Tpcc( executorFactory, (TpccConfig) config, dumpQueryList, queryMode );
                break;
            case "tpch":
                config = new TpchConfig( parsedConfig );
                scenario = new Tpch( executorFactory, (TpchConfig) config, dumpQueryList, queryMode );
                break;
            case "ycsb":
                config = new YcsbConfig( parsedConfig );
                scenario = new Ycsb( executorFactory, (YcsbConfig) config, dumpQueryList, queryMode );
                break;
            default:
                throw new RuntimeException( "Unknown scenario: " + parsedConfig.get( "scenario" ) );
        }

        // Store hostname of node
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "node.hostname" );
            fw.append( hostname );
            fw.close();
            log.warn( "Executing on node: " + hostname );
        } catch ( IOException e ) {
            throw new RuntimeException( "Error while getting hostname", e );
        }

        // Store Simple Client Version for documentation
        try {
            FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "client.version" );
            fw.append( getSimpleClientVersion() );
            fw.close();
        } catch ( IOException e ) {
            log.error( "Error while logging simple client version", e );
        }

        // Stop Polypheny
        polyphenyControlConnector.stopPolypheny();
        try {
            TimeUnit.SECONDS.sleep( 3 );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( "Unexpected interrupt", e );
        }

        DatabaseInstance databaseInstance;
        switch ( config.system ) {
            case "polypheny":
            case "polypheny-jdbc":
            case "polypheny-rest":
            case "polypheny-mongoql":
            case "polypheny-cypher":
                PolyphenyDbExecutor.storeNames.clear();
                databaseInstance = new PolyphenyDbInstance( polyphenyControlConnector, executorFactory, outputDirectory, config );
                scenario.createSchema( databaseInstance, true );
                break;
            case "oltpbench-polypheny":
                PolyphenyDbExecutor.storeNames.clear();
                databaseInstance = new OltpBenchPolyphenyInstance( polyphenyControlConnector, executorFactory, outputDirectory, config );
                scenario.createSchema( databaseInstance, true );
                break;
            case "postgres":
            case "oltpbench-postgres":
                databaseInstance = new PostgresInstance();
                scenario.createSchema( databaseInstance, false );
                break;
            case "monetdb":
                databaseInstance = new MonetdbInstance();
                scenario.createSchema( databaseInstance, false );
                break;
            case "cottontail":
                databaseInstance = new CottontailInstance();
                scenario.createSchema( databaseInstance, false );
                break;
            case "surrealdb":
                databaseInstance = new SurrealDbInstance( ChronosCommand.hostname, "8989" );
                scenario.createSchema( databaseInstance, false );
                break;
            default:
                throw new RuntimeException( "Unknown system: " + config.system );
        }

        if ( databaseInstance instanceof PolyphenyDbInstance polyphenyDbInstance ) {
            // Set workload monitoring
            polyphenyDbInstance.setWorkloadMonitoring( config.workloadMonitoringLoadingData );

            // Start Polypheny status data gathering
            if ( PolyphenyVersionSwitch.getInstance().hasStatusEndpoint ) {
                polyphenyDbInstance.getStatusGatherer().startStatusDataGathering( 60 );
            }
        }

        // Insert data
        log.info( "Inserting data..." );
        ProgressReporter progressReporter = new ChronosProgressReporter(
                chronosJob,
                this,
                scenario.getNumberOfInsertThreads(),
                config.progressReportBase );
        try {
            scenario.generateData( databaseInstance, progressReporter );
        } catch ( Exception e ) {
            databaseInstance.tearDown();
            throw e;
        }

        if ( databaseInstance instanceof PolyphenyDbInstance polyphenyDbInstance && config.restartAfterLoadingData ) {
            polyphenyDbInstance.restartPolypheny();
        }

        return new ImmutableTriple<>( scenario, config, databaseInstance );
    }


    @Override
    protected Object warmUp( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        @SuppressWarnings("unchecked")
        Scenario scenario = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getLeft();
        @SuppressWarnings("unchecked")
        AbstractConfig config = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getMiddle();
        @SuppressWarnings("unchecked")
        DatabaseInstance databaseInstance = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getRight();
        try {
            if ( databaseInstance instanceof PolyphenyDbInstance polyphenyDbInstance ) {
                // Set workload monitoring
                polyphenyDbInstance.setWorkloadMonitoring( config.workloadMonitoringWarmup );

                // Enable icarus training -- to be removed
                if ( config.router != null && config.router.equals( "icarus" ) && PolyphenyVersionSwitch.getInstance().hasIcarusRoutingSettings ) {
                    polyphenyDbInstance.setIcarusRoutingTraining( true );
                }

                // Enable Post Cost Aggregation
                if ( config.postCostAggregation.equals( "onWarmup" ) && !PolyphenyVersionSwitch.getInstance().hasIcarusRoutingSettings ) {
                    polyphenyDbInstance.setPostCostAggregation( true );
                }

                // Wait a moment to give Polypheny-DB the chance to process all data points from data insertion
                try {
                    TimeUnit.MINUTES.sleep( 2 );
                } catch ( InterruptedException e ) {
                    throw new RuntimeException( "Unexpected interrupt", e );
                }
            }

            ProgressReporter progressReporter = new ChronosProgressReporter(
                    chronosJob,
                    this,
                    1,
                    config.progressReportBase );
            scenario.warmUp( progressReporter );

            if ( databaseInstance instanceof PolyphenyDbInstance polyphenyDbInstance ) {
                // Wait a moment to give Polypheny-DB the chance to process all data points from warmup
                try {
                    TimeUnit.MINUTES.sleep( 1 );
                } catch ( InterruptedException e ) {
                    throw new RuntimeException( "Unexpected interrupt", e );
                }

                // Disable Post Cost Aggregation
                if ( config.postCostAggregation.equals( "onWarmup" ) && !PolyphenyVersionSwitch.getInstance().hasIcarusRoutingSettings ) {
                    polyphenyDbInstance.setPostCostAggregation( false );
                }

                // Disable icarus training  -- to be removed
                if ( config.router != null && config.router.equals( "icarus" ) && PolyphenyVersionSwitch.getInstance().hasIcarusRoutingSettings ) {
                    polyphenyDbInstance.setIcarusRoutingTraining( false );
                }
                PolyphenyFullStatus status = polyphenyDbInstance.getStatusGatherer().gatherFullOnce();
                properties.put( "pdbStatus_implementationCacheSize_after_warmup", status.implementationCacheSize() );
                properties.put( "pdbStatus_queryPlanCacheSize_after_warmup", status.queryPlanCacheSize() );
                properties.put( "pdbStatus_routingPlanCacheSize_after_warmup", status.routingPlanCacheSize() );
                properties.put( "pdbStatus_monitoringQueueSize_after_warmup", status.monitoringQueueSize() );
            }
        } catch ( Exception e ) {
            databaseInstance.tearDown();
            throw e;
        }
        return new ImmutableTriple<>( scenario, config, databaseInstance );
    }


    @Override
    protected Object execute( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        @SuppressWarnings("unchecked")
        Scenario scenario = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getLeft();
        @SuppressWarnings("unchecked")
        AbstractConfig config = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getMiddle();
        @SuppressWarnings("unchecked")
        DatabaseInstance databaseInstance = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getRight();

        final CsvWriter csvWriter;
        if ( writeCsv ) {
            csvWriter = new CsvWriter( outputDirectory.getPath() + File.separator + "results.csv" );
        } else {
            csvWriter = null;
        }

        // Set workload monitoring
        if ( databaseInstance instanceof PolyphenyDbInstance polyphenyDbInstance ) {
            polyphenyDbInstance.setWorkloadMonitoring( config.workloadMonitoringExecutingWorkload );
        }

        int numberOfThreads = config.numberOfThreads;
        int maxNumberOfThreads = scenario.getExecutorFactory().getMaxNumberOfThreads();
        if ( maxNumberOfThreads > 0 && config.numberOfThreads > maxNumberOfThreads ) {
            numberOfThreads = maxNumberOfThreads;
            log.warn( "Limiting number of executor threads to {} threads (instead of {} as specified by the job)", numberOfThreads, config.numberOfThreads );
        }
        try {
            ProgressReporter progressReporter = new ChronosProgressReporter(
                    chronosJob,
                    this,
                    numberOfThreads,
                    config.progressReportBase );
            long runtime = scenario.execute( progressReporter, csvWriter, outputDirectory, numberOfThreads );
            properties.put( "runtime", runtime );
        } catch ( Exception e ) {
            databaseInstance.tearDown();
            throw e;
        }

        return new ImmutableTriple<>( scenario, config, databaseInstance );
    }


    @Override
    protected Object analyze( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        @SuppressWarnings("unchecked")
        Scenario scenario = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getLeft();
        @SuppressWarnings("unchecked")
        AbstractConfig config = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getMiddle();
        @SuppressWarnings("unchecked")
        DatabaseInstance databaseInstance = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getRight();

        try {
            scenario.analyze( properties, outputDirectory );
        } catch ( Exception e ) {
            databaseInstance.tearDown();
            throw e;
        }

        if ( databaseInstance instanceof PolyphenyDbInstance polyphenyDbInstance && PolyphenyVersionSwitch.getInstance().hasStatusEndpoint ) {
            StatusGatherer statusGatherer = polyphenyDbInstance.getStatusGatherer();

            // Stop gathering
            List<PolyphenyStatus> statuses = statusGatherer.stopGathering();

            // Analyze and store readings
            List<Long> currentMemoryReadings = new ArrayList<>( statuses.size() );
            List<Integer> numOfActiveTrxReadings = new ArrayList<>( statuses.size() );
            List<Integer> monitoringQueueSizeReadings = new ArrayList<>( statuses.size() );
            for ( PolyphenyStatus status : statuses ) {
                currentMemoryReadings.add( status.currentMemory() );
                numOfActiveTrxReadings.add( status.numOfActiveTrx() );
                monitoringQueueSizeReadings.add( status.monitoringQueueSize() );
            }
            properties.put( "pdbStatus_currentMemory", currentMemoryReadings );
            properties.put( "pdbStatus_numOfActiveTrx", numOfActiveTrxReadings );
            properties.put( "pdbStatus_monitoringQueueSize", monitoringQueueSizeReadings );

            // Wait one minute for garbage collection to run
            try {
                TimeUnit.MINUTES.sleep( 1 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }

            // Do a final gathering
            try {
                PolyphenyFullStatus status = statusGatherer.gatherFullOnce();
                properties.put( "pdbStatus_uuid", status.uui() );
                properties.put( "pdbStatus_version", status.version() );
                properties.put( "pdbStatus_hash", status.hash() );
                properties.put( "pdbStatus_currentMemory_final", status.currentMemory() );
                properties.put( "pdbStatus_numOfActiveTrx_final", status.numOfActiveTrx() );
                properties.put( "pdbStatus_trxCount_final", status.trxCount() );
                properties.put( "pdbStatus_implementationCacheSize_final", status.implementationCacheSize() );
                properties.put( "pdbStatus_queryPlanCacheSize_final", status.queryPlanCacheSize() );
                properties.put( "pdbStatus_routingPlanCacheSize_final", status.routingPlanCacheSize() );
                properties.put( "pdbStatus_monitoringQueueSize_final", status.monitoringQueueSize() );
            } catch ( Exception e ) {
                log.error( "Unable to gather final status data from Polypheny", e );
            }
        }

        return new ImmutableTriple<>( scenario, config, databaseInstance );
    }


    @Override
    protected Object clean( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        @SuppressWarnings("unchecked")
        Scenario scenario = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getLeft();
        @SuppressWarnings("unchecked")
        AbstractConfig config = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getMiddle();
        @SuppressWarnings("unchecked")
        DatabaseInstance databaseInstance = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getRight();

        databaseInstance.tearDown();
        if ( dockerContainerName != null ) {
            DockerLauncher.remove( dockerContainerName );
        }
        return null;
    }


    @Override
    protected void aborted( ChronosJob chronosJob ) {
        if ( getSingleJobId() != null ) {
            try {
                if ( polyphenyControlConnector != null ) {
                    polyphenyControlConnector.stopPolypheny();
                }
            } finally {
                System.exit( 1 );
            }
        }
    }


    @Override
    protected void failed( ChronosJob chronosJob ) {

    }


    public Map<String, String> parseConfig( ChronosJob chronosJob ) {
        Map<String, String> settings;
        try {
            settings = chronosJob.getParsedCdl();
        } catch ( ExecutionException e ) {
            throw new RuntimeException( "Exception while parsing cdl", e );
        }
        return settings;
    }


    @Override
    protected void addChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        ChronosLog4JAppender.setChronosLogHandler( chronosLogHandler );
    }


    @Override
    protected void removeChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        ChronosLog4JAppender.setChronosLogHandler( null );
    }


    void updateProgress( ChronosJob job, int progress ) {
        setProgress( job, (byte) progress );
    }


    private String getSimpleClientVersion() {
        String v = ChronosAgent.class.getPackage().getImplementationVersion();
        if ( v == null ) {
            return "Unknown";
        }
        return v;
    }

}
