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

package org.polypheny.simpleclient.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.AirlineModule;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbRestExecutor.PolyphenyDbRestExecutorFactory;
import org.polypheny.simpleclient.main.MultimediaScenario;


@Slf4j
@Command(name = "multimedia", description = "Mode for quick testing of Polypheny-DB using the multimedia benchmark.")
public class MultimediaCommand implements CliRunnable {

    @AirlineModule
    private HelpOption<MultimediaCommand> help;

    @Arguments(description = "Task { schema | data | workload | warmup } and multiplier.")
    private List<String> args;

    @Option(name = { "-pdb", "--polyphenydb" }, title = "IP or Hostname", arity = 1, description = "IP or Hostname of the Polypheny-DB server (default: 127.0.0.1).")
    public static String polyphenyDbHost = "127.0.0.1";

    @Option(name = { "--rest" }, arity = 0, description = "Use Polypheny-DB REST interface instead of the JDBC interface (default: false).")
    public static boolean restInterface = false;

    @Option(name = { "--writeCSV" }, arity = 0, description = "Write a CSV file containing execution times for all executed queries (default: false).")
    public boolean writeCsv = false;

    @Option(name = { "--queryList" }, arity = 0, description = "Dump all multimedia queries as SQL into a file (default: false).")
    public boolean dumpQueryList = false;


    @Override
    public int run() {
        if ( args == null || args.isEmpty() ) {
            System.err.println( "Missing task" );
            System.exit( 1 );
        }

        int multiplier = 1;
        if ( args.size() > 1 ) {
            multiplier = Integer.parseInt( args.get( 1 ) );
            if ( multiplier < 1 ) {
                System.err.println( "Multiplier needs to be a integer > 0!" );
                System.exit( 1 );
            }
        }

        ExecutorFactory executorFactory;
        if ( restInterface ) {
            executorFactory = new PolyphenyDbRestExecutorFactory( polyphenyDbHost );
        } else {
            executorFactory = new PolyphenyDbJdbcExecutorFactory( polyphenyDbHost, true );
        }

        try {
            if ( args.getFirst().equalsIgnoreCase( "data" ) ) {
                MultimediaScenario.data( executorFactory, multiplier, true );
            } else if ( args.getFirst().equalsIgnoreCase( "workload" ) ) {
                MultimediaScenario.workload( executorFactory, multiplier, true, writeCsv, dumpQueryList );
            } else if ( args.getFirst().equalsIgnoreCase( "schema" ) ) {
                MultimediaScenario.schema( executorFactory, true );
            } else if ( args.getFirst().equalsIgnoreCase( "warmup" ) ) {
                MultimediaScenario.warmup( executorFactory, multiplier, true, dumpQueryList );
            } else {
                System.err.println( "Unknown task: " + args.getFirst() );
            }
        } catch ( Throwable t ) {
            log.error( "Exception while executing MultimediaBench!", t );
            System.exit( 1 );
        }

        try {
            Thread.sleep( 2000 );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( "Unexpected interrupt", e );
        }

        return 0;
    }

}
