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

import ch.unibas.dmi.dbis.chronos.agent.AbstractChronosAgent;
import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.inject.Inject;
import org.polypheny.simpleclient.main.ChronosAgent;


@Command(name = "chronos", description = "Start as a Chronos Agent")
public class ChronosCommand implements CliRunnable {

    @SuppressWarnings("SpringAutowiredFieldsWarningInspection")
    @Inject
    private HelpOption<ChronosCommand> help;

    @Option(name = { "-cc", "--chronos" }, description = "Hostname or IP-Address of the Chronos Control (default: chronos.dmi.unibas.ch).")
    private String chronos = "chronos.dmi.unibas.ch";

    @Option(name = { "-p", "--port" }, description = "Port of the REST API of the Chronos Control server (default: 443).")
    private int port = 443;

    @Option(name = { "-e", "--environment" }, description = "Identifier of the Chronos evaluation environment this client belongs to (default: \"default\").")
    private String environment = "default";

    @Required
    @Option(name = { "-s", "--supports" }, description = "Comma-separated list of system identifiers supported by this client. Depends on the Chronos instance.")
    private String supports = "";

    @Option(name = { "--host" }, title = "IP or Port", description = "Hostname or IP address of the host running the system(s) to be benchmarked (default: 127.0.0.1).")
    public static String hostname = "127.0.0.1";

    @Option(name = { "--writeCSV" }, arity = 0, description = "Write a CSV file containing execution times for all executed queries (default: false).")
    public boolean writeCsv = false;

    @Option(name = { "--queryList" }, arity = 0, description = "Dump all queries into a file (default: false).")
    public boolean dumpQueryList = false;


    @Override
    public int run() {
        InetAddress address = null;
        try {
            address = InetAddress.getByName( chronos );
        } catch ( UnknownHostException e ) {
            System.err.println( "The given host '" + chronos + "' cannot be resolved." );
            System.exit( 1 );
        }

        AbstractChronosAgent aca = new ChronosAgent( address, port, true, true, environment, supports.split( "," ), writeCsv, dumpQueryList );
        aca.setDaemon( false );
        aca.start();

        return 0;
    }

}
