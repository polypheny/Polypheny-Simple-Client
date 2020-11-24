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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.cli.ChronosCommand;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.JdbcExecutor;
import org.polypheny.simpleclient.executor.MonetdbExecutor;
import org.polypheny.simpleclient.executor.MonetdbExecutor.MonetdbExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbRestExecutor.PolyphenyDbRestExecutorFactory;
import org.polypheny.simpleclient.executor.PostgresExecutor;
import org.polypheny.simpleclient.executor.PostgresExecutor.PostgresExecutorFactory;
import org.polypheny.simpleclient.scenario.Scenario;
import org.polypheny.simpleclient.scenario.gavel.Config;
import org.polypheny.simpleclient.scenario.gavel.Gavel;


@Slf4j
public class ChronosAgent extends AbstractChronosAgent {

    // Whether the individual execution time of every query should be stored in the result json
    // This massively increases the amount of data stored in the Chronos Control database
    public static final boolean STORE_INDIVIDUAL_QUERY_TIMES = false;

    public final String[] supports;
    private PolyphenyControlConnector polyphenyControlConnector = null;

    private final boolean writeCsv;
    private final boolean dumpQueryList;


    public ChronosAgent( InetAddress address, int port, boolean secure, boolean useHostname, String environment, String supports, boolean writeCsv, boolean dumpQueryList ) {
        super( address, port, secure, useHostname, environment );
        this.writeCsv = writeCsv;
        this.dumpQueryList = dumpQueryList;
        this.supports = new String[]{ supports };
        try {
            polyphenyControlConnector = new PolyphenyControlConnector( ChronosCommand.hostname + ":8070" );
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
        Config config = parseConfig( chronosJob );

        // Create Executor Factory
        Executor.ExecutorFactory executorFactory;
        switch ( config.system ) {
            case "polypheny":
                executorFactory = new PolyphenyDbJdbcExecutorFactory( ChronosCommand.hostname );
                break;
            case "polypheny-rest":
                executorFactory = new PolyphenyDbRestExecutorFactory( ChronosCommand.hostname );
                break;
            case "postgres":
                executorFactory = new PostgresExecutorFactory( ChronosCommand.hostname );
                break;
            case "monetdb":
                executorFactory = new MonetdbExecutorFactory( ChronosCommand.hostname );
                break;
            default:
                throw new RuntimeException( "Unknown system: " + config.system );
        }

        Scenario scenario = new Gavel( executorFactory, config, true, dumpQueryList );

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

        if ( config.system.equals( "polypheny" ) || config.system.equals( "polypheny-rest" ) ) {
            // Update settings
            Map<String, String> conf = new HashMap<>();
            conf.put( "pcrtl.pdbms.branch", config.pdbBranch.trim() );
            conf.put( "pcrtl.ui.branch", config.puiBranch.trim() );
            conf.put( "pcrtl.java.heap", "10" );
            conf.put( "pcrtl.buildmode", "both" );
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
            try {
                TimeUnit.SECONDS.sleep( 10 );
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
            PolyphenyDbExecutor executor = (PolyphenyDbExecutor) executorFactory.createInstance();
            try {
                // Remove hsqldb store
                executor.dropStore( "hsqldb" );
                // Deploy store
                switch ( config.dataStore ) {
                    case "hsqldb":
                        executor.deployHsqldb();
                        break;
                    case "postgres":
                        resetPostgres();
                        executor.deployPostgres();
                        break;
                    case "monetdb":
                        resetMonetDb();
                        executor.deployMonetDb();
                        break;
                    case "cassandra":
                        executor.deployCassandra();
                        break;
                    case "file":
                        executor.deployFileStore();
                        break;
                    case "cottontail":
                        executor.deployCottontail();
                        break;
                    case "monetdb+postgres":
                        resetPostgres();
                        resetMonetDb();
                        executor.deployPostgres();
                        executor.deployMonetDb();
                        break;
                    case "all":
                        resetPostgres();
                        resetMonetDb();
                        executor.deployHsqldb();
                        executor.deployPostgres();
                        executor.deployMonetDb();
                        executor.deployCassandra();
                        executor.deployFileStore();
                        executor.deployCottontail();
                        break;
                    default:
                        throw new RuntimeException( "Unknown data store: " + config.dataStore );
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
            executor = (PolyphenyDbExecutor) executorFactory.createInstance();
            try {
                // disable active tracking (dynamic querying)
                executor.setConfig( "statistics/activeTracking", "false" );
                // Set router
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

            // Create schema
            scenario.createSchema( true );
        } else if ( config.system.equals( "postgres" ) ) {
            resetPostgres();
            scenario.createSchema( false );
        } else if ( config.system.equals( "monetdb" ) ) {
            resetMonetDb();
            scenario.createSchema( false );
        }

        // Insert data
        log.info( "Inserting data..." );
        int numberOfThreads = config.numberOfUserGenerationThreads + config.numberOfAuctionGenerationThreads;
        ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, numberOfThreads, config.progressReportBase );
        scenario.generateData( progressReporter );

        return scenario;
    }


    @Override
    protected Object warmUp( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        Config config = parseConfig( chronosJob );
        Scenario scenario = (Scenario) o;

        // enable icarus training
        if ( config.system.equals( "polypheny" ) && config.router.equals( "icarus" ) ) {
            setIcarusRoutingTraining( true );
        }

        ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, 1, config.progressReportBase );
        scenario.warmUp( progressReporter, config.numberOfWarmUpIterations );

        // disable icarus training
        if ( config.system.equals( "polypheny" ) && config.router.equals( "icarus" ) ) {
            setIcarusRoutingTraining( false );
        }

        return scenario;
    }


    @Override
    protected Object execute( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        Config config = parseConfig( chronosJob );
        Scenario scenario = (Scenario) o;

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
        ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, numberOfThreads, config.progressReportBase );
        long runtime = scenario.execute( progressReporter, csvWriter, outputDirectory, numberOfThreads );
        properties.put( "runtime", runtime );

        return scenario;
    }


    @Override
    protected Object analyze( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        Scenario scenario = (Scenario) o;

        scenario.analyze( properties );

        return scenario;
    }


    @Override
    protected Object clean( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        return null;
    }


    @Override
    protected void aborted( ChronosJob chronosJob ) {

    }


    @Override
    protected void failed( ChronosJob chronosJob ) {

    }


    @Override
    protected void addChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        ChronosLog4JAppender.setChronosLogHandler( chronosLogHandler );
    }


    @Override
    protected void removeChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        ChronosLog4JAppender.setChronosLogHandler( null );
    }


    void setIcarusRoutingTraining( boolean b ) {
        PolyphenyDbExecutor executor = (PolyphenyDbExecutor) new PolyphenyDbJdbcExecutorFactory( ChronosCommand.hostname ).createInstance();
        try {
            // disable icarus training
            executor.setConfig( "icarusRouting/training", b ? "true" : "false" );
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


    void updateProgress( ChronosJob job, int progress ) {
        setProgress( job, (byte) progress );
    }


    private void resetPostgres() {
        JdbcExecutor postgresExecutor = new PostgresExecutor( ChronosCommand.hostname, null );
        try {
            postgresExecutor.reset();
            postgresExecutor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while dropping tables on postgres", e );
        } finally {
            try {
                postgresExecutor.closeConnection();
            } catch ( ExecutorException e ) {
                log.error( "Exception while closing connection", e );
            }
        }
    }


    private void resetMonetDb() {
        JdbcExecutor monetdbExecutor = new MonetdbExecutor( ChronosCommand.hostname, null );
        try {
            monetdbExecutor.reset();
            monetdbExecutor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while dropping tables on monetdb", e );
        } finally {
            try {
                monetdbExecutor.closeConnection();
            } catch ( ExecutorException e ) {
                log.error( "Exception while closing connection", e );
            }
        }
    }


    private Config parseConfig( ChronosJob chronosJob ) {
        Map<String, String> settings;
        try {
            settings = chronosJob.getParsedCdl();
        } catch ( ExecutionException e ) {
            throw new RuntimeException( "Exception while parsing cdl", e );
        }
        return new Config( settings );
    }


    private String getSimpleClientVersion() {
        String v = ChronosAgent.class.getPackage().getImplementationVersion();
        if ( v == null ) {
            return "Unknown";
        }
        return v;
    }

}
