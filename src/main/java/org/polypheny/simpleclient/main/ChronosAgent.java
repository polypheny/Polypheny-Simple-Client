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
import org.polypheny.simpleclient.cli.Main;
import org.polypheny.simpleclient.scenario.Scenario;
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
        Config config = parseConfig( chronosJob );
        Scenario scenario = new Gavel( ChronosCommand.polyphenyDbHost, config );

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

        // Store Simple Client Version for documentation
        try {
            FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "client.version" );
            fw.append( getSimpleClientVersion() );
            fw.close();
        } catch ( IOException e ) {
            log.error( "Error while logging simple client version", e );
        }

        // Create schema
        scenario.createSchema();

        // Insert data
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
        if ( config.store.equals( "polypheny" ) ) {
            scenario.warmUp( progressReporter );
        } else {
            if ( config.store.equals( "postgres" ) ) {
                scenario.warmUp( progressReporter );
            } else {
                System.err.println( "Unknown Store: " + config.store );
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
        if ( config.store.equals( "polypheny" ) ) {
            runtime = scenario.execute( progressReporter, csvWriter, outputDirectory );
        } else if ( config.store.equals( "postgres" ) ) {
            runtime = scenario.execute( progressReporter, csvWriter, outputDirectory );
        } else {
            System.err.println( "Unknown Store: " + config.store );
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