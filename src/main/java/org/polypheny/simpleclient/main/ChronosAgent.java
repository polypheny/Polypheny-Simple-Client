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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.cli.ChronosCommand;
import org.polypheny.simpleclient.cli.Main;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.MonetdbExecutor;
import org.polypheny.simpleclient.executor.MonetdbExecutor.MonetdbExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor.PolyphenyDBExecutorFactory;
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
    PolyphenyControlConnector polyphenyControlConnector = null;


    public ChronosAgent( InetAddress address, int port, boolean secure, boolean useHostname, String environment, String supports ) {
        super( address, port, secure, useHostname, environment );
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
        if ( config.system.equals( "polypheny" ) ) {
            executorFactory = new PolyphenyDBExecutorFactory( ChronosCommand.hostname );
        } else if ( config.system.equals( "postgres" ) ) {
            executorFactory = new PostgresExecutorFactory( ChronosCommand.hostname );
        } else if ( config.system.equals( "monetdb" ) ) {
            executorFactory = new MonetdbExecutorFactory( ChronosCommand.hostname );
        } else {
            throw new RuntimeException( "Unknown system: " + config.system );
        }

        Scenario scenario = new Gavel( executorFactory, config );

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

        if ( config.system.equals( "polypheny" ) ) {
            // Stop Polypheny
            polyphenyControlConnector.stopPolypheny();
            try {
                TimeUnit.SECONDS.sleep( 3 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }

            // Update settings
            Map<String, String> conf = new HashMap<>();
            conf.put( "pcrtl.pdbms.branch", config.pdbBranch.trim() );
            conf.put( "pcrtl.ui.branch", config.puiBranch.trim() );
            conf.put( "pcrtl.java.heap", "10" );
            conf.put( "pcrtl.buildmode", "both" );
            conf.put( "pcrtl.clean", "keep" );
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
            Executor executor = executorFactory.createInstance();
            try {
                // Remove hsqldb store
                executor.executeStatement( new Query( "ALTER STORES DROP hsqldb", false ) );
                if ( config.dataStore.equals( "hsqldb" ) ) {
                    executor.executeStatement( new Query( "alter stores add foo using 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore' with '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory}'", false ) );
                } else if ( config.dataStore.equals( "postgres" ) ) {
                    executor.executeStatement( new Query( "alter stores add postgres using 'org.polypheny.db.adapter.jdbc.stores.PostgresqlStore' with '{\"database\":\"test\",\"host\":\"localhost\",\"maxConnections\":\"25\",\"password\":\"postgres\",\"username\":\"postgres\",\"port\":\"5432\"}'", false ) );
                    // Drop all existing tables
                    Executor postgresExecutor = new PostgresExecutor( ChronosCommand.hostname );
                    try {
                        postgresExecutor.reset();
                        postgresExecutor.executeCommit();
                    } catch ( SQLException e ) {
                        throw new RuntimeException( "Exception while dropping tables on postgres", e );
                    } finally {
                        try {
                            postgresExecutor.closeConnection();
                        } catch ( SQLException e ) {
                            log.error( "Exception while closing connection", e );
                        }
                    }
                } else if ( config.dataStore.equals( "monetdb" ) ) {
                    executor.executeStatement( new Query( "alter stores add postgres using 'org.polypheny.db.adapter.jdbc.stores.MonetdbStore' with '{\"database\":\"test\",\"host\":\"localhost\",\"maxConnections\":\"25\",\"password\":\"monetdb\",\"username\":\"monetdb\",\"port\":\"50000\"}'", false ) );
                    // Drop all existing tables
                    Executor monetdbExecutor = new MonetdbExecutor( ChronosCommand.hostname );
                    try {
                        monetdbExecutor.reset();
                        monetdbExecutor.executeCommit();
                    } catch ( SQLException e ) {
                        throw new RuntimeException( "Exception while dropping tables on monetdb", e );
                    } finally {
                        try {
                            monetdbExecutor.closeConnection();
                        } catch ( SQLException e ) {
                            log.error( "Exception while closing connection", e );
                        }
                    }
                } else if ( config.dataStore.equals( "cassandra" ) ) {
                    executor.executeStatement( new Query( "alter stores add cassandra using 'org.polypheny.db.adapter.cassandra.CassandraStore' with '{\"type\":\"Embedded\",\"host\":\"localhost\",\"port\":\"9042\",\"keyspace\":\"cassandra\",\"username\":\"cassandra\",\"password\":\"cass\"}'\n", false ) );
                }
                executor.executeCommit();
            } catch ( SQLException e ) {
                throw new RuntimeException( "Exception while configuring stores", e );
            } finally {
                try {
                    executor.closeConnection();
                } catch ( SQLException e ) {
                    log.error( "Exception while closing connection", e );
                }
            }
            // Create schema
            scenario.createSchema( true );
        } else if ( config.system.equals( "postgres" ) ) {
            // Drop all existing tables
            Executor executor = executorFactory.createInstance();
            try {
                executor.reset();
                executor.executeCommit();
            } catch ( SQLException e ) {
                throw new RuntimeException( "Exception while dropping tables on postgres", e );
            } finally {
                try {
                    executor.closeConnection();
                } catch ( SQLException e ) {
                    log.error( "Exception while closing connection", e );
                }
            }
            // Create schema
            scenario.createSchema( false );
        } else if ( config.system.equals( "monetdb" ) ) {
            // Drop all existing tables
            Executor executor = executorFactory.createInstance();
            try {
                executor.reset();
                executor.executeCommit();
            } catch ( SQLException e ) {
                throw new RuntimeException( "Exception while dropping tables on monetdb", e );
            } finally {
                try {
                    executor.closeConnection();
                } catch ( SQLException e ) {
                    log.error( "Exception while closing connection", e );
                }
            }
            // Create schema
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

        ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, config.numberOfThreads, config.progressReportBase );
        if ( config.system.equals( "polypheny" ) ) {
            scenario.warmUp( progressReporter );
        } else {
            if ( config.system.equals( "postgres" ) ) {
                scenario.warmUp( progressReporter );
            } else if ( config.system.equals( "monetdb" ) ) {
                scenario.warmUp( progressReporter );
            } else {
                throw new RuntimeException( "Unknown Store: " + config.system );
            }
        }
        return scenario;
    }


    @Override
    protected Object execute( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        Config config = parseConfig( chronosJob );
        Scenario scenario = (Scenario) o;

        final CsvWriter csvWriter;
        if ( Main.WRITE_CSV ) {
            csvWriter = new CsvWriter( outputDirectory.getPath() + File.separator + "results.csv" );
        } else {
            csvWriter = null;
        }
        ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, config.numberOfThreads, config.progressReportBase );
        long runtime = 0;
        if ( config.system.equals( "polypheny" ) ) {
            runtime = scenario.execute( progressReporter, csvWriter, outputDirectory );
        } else if ( config.system.equals( "postgres" ) ) {
            runtime = scenario.execute( progressReporter, csvWriter, outputDirectory );
        } else if ( config.system.equals( "monetdb" ) ) {
            runtime = scenario.execute( progressReporter, csvWriter, outputDirectory );
        } else {
            throw new RuntimeException( "Unknown Store: " + config.system );
        }
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


    void updateProgress( ChronosJob job, int progress ) {
        setProgress( job, (byte) progress );
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