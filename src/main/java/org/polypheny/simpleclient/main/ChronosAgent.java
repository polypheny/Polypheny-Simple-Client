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
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.executor.PostgresExecutor;
import org.polypheny.simpleclient.scenario.gavel.Config;
import org.polypheny.simpleclient.scenario.gavel.Gavel;


@Slf4j
public class ChronosAgent extends AbstractChronosAgent {


    public final String[] supports;
    PolyphenyControlConnector polyphenyControlConnector = null;


    public ChronosAgent( InetAddress address, int port, boolean secure, boolean useHostname, String environment, String supports ) {
        super( address, port, secure, useHostname, environment );
        this.supports = new String[]{ supports };
        try {
            polyphenyControlConnector = new PolyphenyControlConnector( ChronosCommand.polyphenyDbHost + ":8070" );
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
        Map<String, String> settings;
        try {
            settings = chronosJob.getParsedCdl();
        } catch ( ExecutionException e ) {
            throw new RuntimeException( "Exception while parsing cdl", e );
        }
        Config config = new Config( settings );
        Gavel gavel = new Gavel( ChronosCommand.polyphenyDbHost, config );

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

        // Create schema
        try {
            gavel.createSchema();
        } catch ( SQLException e ) {
            log.error( "Error while creating schema", e );
        }

        // Insert data
        int numberOfThreads = config.numberOfUserGenerationThreads + config.numberOfAuctionGenerationThreads;
        ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, numberOfThreads, config.progressReportBase );
        gavel.buildDatabase( progressReporter );

        return config;
    }


    @Override
    protected Object warmUp( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        Config config = (Config) o;
        Gavel gavel = new Gavel( ChronosCommand.polyphenyDbHost, config );

        ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, config.numberOfThreads, config.progressReportBase );
        if ( config.store.equals( "polypheny" ) ) {
            gavel.warmUp( progressReporter, new PolyphenyDbExecutor( ChronosCommand.polyphenyDbHost, config ) );
        } else {
            if ( config.store.equals( "postgres" ) ) {
                gavel.warmUp( progressReporter, new PostgresExecutor( ChronosCommand.polyphenyDbHost ) );
            } else {
                System.err.println( "Unknown Store: " + config.store );
            }
        }
        return config;
    }


    @Override
    protected Object execute( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        Config config = (Config) o;
        Gavel gavel = new Gavel( ChronosCommand.polyphenyDbHost, config );

        final CsvWriter csvWriter;
        if ( Main.WRITE_CSV ) {
            csvWriter = new CsvWriter( outputDirectory.getPath() + File.separator + "results.csv" );
        } else {
            csvWriter = null;
        }
        ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, config.numberOfThreads, config.progressReportBase );
        long runtime = 0;
        if ( config.store.equals( "polypheny" ) ) {
            runtime = gavel.execute( progressReporter, csvWriter, outputDirectory, new PolyphenyDbExecutor( ChronosCommand.polyphenyDbHost, config ) );
            gavel.analyze( properties );
        } else {
            if ( config.store.equals( "postgres" ) ) {
                runtime = gavel.execute( progressReporter, csvWriter, outputDirectory, new PostgresExecutor( ChronosCommand.polyphenyDbHost ) );
            } else {
                System.err.println( "Unknown Store: " + config.store );
            }
        }
        gavel.analyzeMeasuredTime( properties );
        properties.put( "runtime", runtime );
        log.info( gavel.getTimesAsString( properties ) );

        return config;
    }


    @Override
    protected Object analyze( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        return o;
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


    void updateProgress( ChronosJob job, int progress ) {
        setProgress( job, (byte) progress );
    }


    @Override
    protected void addChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        polyphenyControlConnector.setChronosLogHandler( chronosLogHandler );
    }


    @Override
    protected void removeChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        polyphenyControlConnector.setChronosLogHandler( null );
    }

}