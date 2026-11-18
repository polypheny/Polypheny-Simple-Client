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
 *
 */

package org.polypheny.simpleclient.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.AirlineModule;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Arrays;
import org.polypheny.simpleclient.main.ChronosAgent;


@Command(name = "chronos", description = "Start as a Chronos Agent")
public class ChronosCommand implements CliRunnable {

    @AirlineModule
    private HelpOption<ChronosCommand> help;

    @Option(name = { "-cc", "--chronos" }, description = "Hostname or IP-Address of the Chronos Control (default: chronos.dmi.unibas.ch).")
    private String chronos = "chronos.dmi.unibas.ch";

    @Option(name = { "-p", "--port" }, description = "Port of the REST API of the Chronos Control server (default: 443).")
    private int port = 443;

    @Option(name = { "-e", "--environment" }, description = "Identifier of the Chronos evaluation environment this client belongs to (default: \"default\").")
    private String environment = "default";

    @Option(name = { "-s", "--supports" }, description = "Comma-separated list of system identifiers supported by this client. Depends on the Chronos instance.")
    private String supports = null;

    @Option(name = { "--host" }, title = "IP or Port", description = "Hostname or IP address of the host running the system(s) to be benchmarked (default: 127.0.0.1).")
    public static String hostname = "127.0.0.1";

    @Option(name = { "--control-username" }, title = "Polypheny Control Username", description = "The username to be used for authentication with Polypheny Control.")
    public static String controlUsername = "";

    @Option(name = { "--control-password" }, title = "Polypheny Control Password", description = "The password to be used for authentication with Polypheny Control.")
    public static String controlPassword = "";

    @Option(name = { "--writeCSV" }, arity = 0, description = "Write a CSV file containing execution times for all executed queries (default: false).")
    public boolean writeCsv = false;

    @Option(name = { "--queryList" }, arity = 0, description = "Dump all queries into a file (default: false).")
    public boolean dumpQueryList = false;

    @Option(name = { "--job" }, title = "Id of the job to run", description = "Run a single chronos job (default: null)")
    public Integer jobId = null;

    @Option(name = { "--buildserver" }, title = "buildserver URL", description = "Use a buildserver to download JAR files and run them directly")
    public String buildServer = null;

    @Option(name = { "--ycsb-config" }, description = "Run Docker YCSB using this local configuration file.")
    private String ycsbConfig;


    @Option(name = { "--ch2-config" }, description = "Run Docker CH2 using this local configuration file.")
    private String ch2Config;

    @Override
    public int run() {
        boolean secure = true;
        URI uri = URI.create( chronos );
        switch ( uri.getScheme() ) {
            case "http":
                chronos = uri.getHost();
                port = uri.getPort() != -1 ? uri.getPort() : 80;
                secure = false;
                break;
            case "https":
                chronos = uri.getHost();
                port = uri.getPort() != -1 ? uri.getPort() : 443;
                break;
            case null: // No scheme means old-style passing of just the hostname
                break;
            default:
                System.err.println( "The given host '" + chronos + "' has an unsupported scheme." );
                System.exit( 1 );
                break;
        }

        InetAddress address = null;
        try {
            address = InetAddress.getByName( chronos );
        } catch ( UnknownHostException e ) {
            System.err.println( "The given host '" + chronos + "' cannot be resolved." );
            System.exit( 1 );
        }

        if ( supports == null && jobId == null ) {
            supports = System.getenv( "CHRONOS_SUPPORTS" );
        }

        if ( (supports == null || supports.isBlank()) && jobId == null ) {
            System.err.println( "Set --supports or CHRONOS_SUPPORTS for polling, or specify --job" );
            System.exit( 1 );
        }

        if ( supports != null && jobId != null ) {
            System.err.println( "--supports and --job are mutually exclusive" );
            System.exit( 1 );
        }

        String[] supportedSystems = supports == null ? new String[0] : Arrays.stream( supports.split( "," ) )
                .map( String::trim )
                .filter( system -> !system.isEmpty() )
                .toArray( String[]::new );
        if ( jobId == null && supportedSystems.length == 0 ) {
            System.err.println( "At least one Chronos system identifier is required" );
            System.exit( 1 );
        }
        ChronosAgent aca = new ChronosAgent( address, port, secure, true, environment, supportedSystems,
                writeCsv, dumpQueryList, buildServer,
                Path.of( ycsbConfig != null ? ycsbConfig : "docker/ycsb/experiment.properties" ),
                Path.of( ch2Config != null ? ch2Config : "docker/ch2/experiment.properties" ) );
        aca.setDaemon( false );
        if ( jobId != null ) {
            aca.setSingleJobId( jobId );
        }
        java.util.concurrent.atomic.AtomicBoolean agentFailed = new java.util.concurrent.atomic.AtomicBoolean();
        aca.setUncaughtExceptionHandler( (thread, error) -> {
            agentFailed.set( true );
            error.printStackTrace();
        } );
        Thread shutdownHook = new Thread( () -> {
            aca.shutdown();
            try {
                aca.join( 90000 );
            } catch ( InterruptedException e ) {
                Thread.currentThread().interrupt();
            }
        }, "chronos-shutdown" );
        Runtime.getRuntime().addShutdownHook( shutdownHook );
        aca.start();
        while ( true ) {
            try {
                aca.join();
                break;
            } catch ( InterruptedException e ) {
                // ignore
            }
        }

        Runtime.getRuntime().removeShutdownHook( shutdownHook );
        if ( jobId != null ) {
            // Chronos dependencies may leave background threads running after the agent joins.
            System.exit( agentFailed.get() || aca.hasFailed() ? 1 : 0 );
        }
        if ( agentFailed.get() ) {
            System.exit( 1 );
        }
        return 0;
    }

}
