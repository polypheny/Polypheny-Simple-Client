/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
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
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor.PolyphenyDbInstance;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbRestExecutor.PolyphenyDbRestExecutorFactory;
import org.polypheny.simpleclient.executor.PostgresExecutor.PostgresExecutorFactory;
import org.polypheny.simpleclient.executor.PostgresExecutor.PostgresInstance;
import org.polypheny.simpleclient.scenario.AbstractConfig;
import org.polypheny.simpleclient.scenario.Scenario;
import org.polypheny.simpleclient.scenario.gavel.Gavel;
import org.polypheny.simpleclient.scenario.gavel.GavelConfig;
import org.polypheny.simpleclient.scenario.knnbench.KnnBench;
import org.polypheny.simpleclient.scenario.knnbench.KnnBenchConfig;
import org.polypheny.simpleclient.scenario.multimedia.MultimediaBench;
import org.polypheny.simpleclient.scenario.multimedia.MultimediaConfig;
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
            polyphenyControlConnector = new PolyphenyControlConnector( ChronosCommand.hostname + ":8070", ClientType.BENCHMARKER, logHandler );
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

        switch ( parsedConfig.get( "queryViews" ) ) {
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
                queryMode = QueryMode.TABLE;
        }

        // Create Executor Factory
        Executor.ExecutorFactory executorFactory;
        switch ( parsedConfig.get( "store" ) ) {
            case "polypheny":
                executorFactory = new PolyphenyDbJdbcExecutorFactory( ChronosCommand.hostname, Boolean.parseBoolean( parsedConfig.get( "prepareStatements" ) ) );
                break;
            case "polypheny-rest":
                executorFactory = new PolyphenyDbRestExecutorFactory( ChronosCommand.hostname );
                break;
            case "postgres":
                executorFactory = new PostgresExecutorFactory( ChronosCommand.hostname, Boolean.parseBoolean( parsedConfig.get( "prepareStatements" ) ) );
                break;
            case "monetdb":
                executorFactory = new MonetdbExecutorFactory( ChronosCommand.hostname, Boolean.parseBoolean( parsedConfig.get( "prepareStatements" ) ) );
                break;
            case "cottontail":
                executorFactory = new CottontailExecutorFactory();
                break;
            default:
                throw new RuntimeException( "Unknown system: " + parsedConfig.get( "store" ) );
        }

        Scenario scenario;
        AbstractConfig config;
        switch ( parsedConfig.get( "scenario" ) ) {
            case "gavel":
                config = new GavelConfig( parsedConfig );
                log.info( "inside chronosAgent" );
                scenario = new Gavel( executorFactory, (GavelConfig) config, true, dumpQueryList, queryMode );
                break;
            case "knnBench":
                config = new KnnBenchConfig( parsedConfig );
                scenario = new KnnBench( executorFactory, (KnnBenchConfig) config, true, dumpQueryList );
                break;
            case "multimedia":
                config = new MultimediaConfig( parsedConfig );
                scenario = new MultimediaBench( executorFactory, (MultimediaConfig) config, true, dumpQueryList );
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
            case "polypheny-rest":
                databaseInstance = new PolyphenyDbInstance( polyphenyControlConnector, executorFactory, outputDirectory, config );
                scenario.createSchema( true );
                break;
            case "postgres":
                databaseInstance = new PostgresInstance();
                scenario.createSchema( false );
                break;
            case "monetdb":
                databaseInstance = new MonetdbInstance();
                scenario.createSchema( false );
                break;
            case "cottontail":
                databaseInstance = new CottontailInstance();
                scenario.createSchema( false );
                break;
            default:
                throw new RuntimeException( "Unknown system: " + config.system );
        }

        // Create View / MaterializedView
        log.info( "Creating View / Materialized View..." );
        if ( queryMode.equals( QueryMode.VIEW ) || queryMode.equals( QueryMode.MATERIALIZED ) ) {
            scenario.createView( queryMode );
        }

        // Insert data
        log.info( "Inserting data..." );
        ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, scenario.getNumberOfInsertThreads(), config.progressReportBase );
        try {
            scenario.generateData( progressReporter );
        } catch ( Exception e ) {
            databaseInstance.tearDown();
            throw e;
        }

        return new ImmutableTriple<>( scenario, config, databaseInstance );
    }


    @Override
    protected Object warmUp( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        @SuppressWarnings("unchecked") Scenario scenario = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getLeft();
        @SuppressWarnings("unchecked") AbstractConfig config = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getMiddle();
        @SuppressWarnings("unchecked") DatabaseInstance databaseInstance = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getRight();
        try {
            // enable icarus training
            if ( config.system.equals( "polypheny" ) && config.router.equals( "icarus" ) ) {
                ((PolyphenyDbInstance) databaseInstance).setIcarusRoutingTraining( true );
            }

            ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, 1, config.progressReportBase );
            scenario.warmUp( progressReporter, config.numberOfWarmUpIterations );

            // disable icarus training
            if ( config.system.equals( "polypheny" ) && config.router.equals( "icarus" ) ) {
                ((PolyphenyDbInstance) databaseInstance).setIcarusRoutingTraining( false );
            }
        } catch ( Exception e ) {
            databaseInstance.tearDown();
            throw e;
        }
        return new ImmutableTriple<>( scenario, config, databaseInstance );
    }


    @Override
    protected Object execute( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        @SuppressWarnings("unchecked") Scenario scenario = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getLeft();
        @SuppressWarnings("unchecked") AbstractConfig config = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getMiddle();
        @SuppressWarnings("unchecked") DatabaseInstance databaseInstance = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getRight();

        final CsvWriter csvWriter;
        if ( writeCsv ) {
            csvWriter = new CsvWriter( outputDirectory.getPath() + File.separator + "results.csv" );
        } else {
            csvWriter = null;
        }

        int numberOfThreads = config.numberOfThreads;
        int maxNumberOfThreads = scenario.getExecutorFactory().getMaxNumberOfThreads();
        if ( maxNumberOfThreads > 0 && config.numberOfThreads > maxNumberOfThreads ) {
            numberOfThreads = maxNumberOfThreads;
            log.warn( "Limiting number of executor threads to {} threads (instead of {} as specified by the job)", numberOfThreads, config.numberOfThreads );
        }
        try {
            ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, numberOfThreads, config.progressReportBase );
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
        @SuppressWarnings("unchecked") Scenario scenario = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getLeft();
        @SuppressWarnings("unchecked") AbstractConfig config = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getMiddle();
        @SuppressWarnings("unchecked") DatabaseInstance databaseInstance = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getRight();

        try {
            scenario.analyze( properties );
        } catch ( Exception e ) {
            databaseInstance.tearDown();
            throw e;
        }

        return new ImmutableTriple<>( scenario, config, databaseInstance );
    }


    @Override
    protected Object clean( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        @SuppressWarnings("unchecked") Scenario scenario = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getLeft();
        @SuppressWarnings("unchecked") AbstractConfig config = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getMiddle();
        @SuppressWarnings("unchecked") DatabaseInstance databaseInstance = ((Triple<Scenario, AbstractConfig, DatabaseInstance>) o).getRight();
        databaseInstance.tearDown();
        return null;
    }


    @Override
    protected void aborted( ChronosJob chronosJob ) {

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
