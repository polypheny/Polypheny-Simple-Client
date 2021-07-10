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

package org.polypheny.simpleclient.cli;


import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbRestExecutor.PolyphenyDbRestExecutorFactory;
import org.polypheny.simpleclient.main.Easy;


@Slf4j
@Command(name = "easy", description = "Mode for quick testing of Polypheny-DB using the Gavel benchmark.")
public class EasyCommand implements CliRunnable {

    @SuppressWarnings("SpringAutowiredFieldsWarningInspection")
    @Inject
    private HelpOption<EasyCommand> help;

    @Arguments(description = "Task { schema | data | viewWorkload | workload | warmup } and multiplier.")
    private List<String> args;

    @Option(name = { "-pdb", "--polyphenydb" }, title = "IP or Hostname", arity = 1, description = "IP or Hostname of the Polypheny-DB server (default: 127.0.0.1).")
    public static String polyphenyDbHost = "127.0.0.1";

    @Option(name = { "--rest" }, arity = 0, description = "Use Polypheny-DB REST interface instead of the JDBC interface (default: false).")
    public static boolean restInterface = false;

    @Option(name = { "--writeCSV" }, arity = 0, description = "Write a CSV file containing execution times for all executed queries (default: false).")
    public boolean writeCsv = false;

    @Option(name = { "--queryList" }, arity = 0, description = "Dump all Gavel queries as SQL into a file (default: false).")
    public boolean dumpQueryList = false;


    @Override
    public int run() {
        if ( args == null || args.size() < 1 ) {
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
            if ( args.get( 0 ).equalsIgnoreCase( "data" ) ) {
                Easy.data( executorFactory, multiplier, true );
            } else if ( args.get( 0 ).equalsIgnoreCase( "workload" ) ) {
                Easy.workload( executorFactory, multiplier, true, writeCsv, dumpQueryList, false );
            } else if(args.get( 0 ).equalsIgnoreCase( "viewWorkload" )){
                Easy.workload( executorFactory, multiplier, true, writeCsv, dumpQueryList, true );
            } else if ( args.get( 0 ).equalsIgnoreCase( "schema" ) ) {
                Easy.schema( executorFactory, true );
            } else if ( args.get( 0 ).equalsIgnoreCase( "warmup" ) ) {
                Easy.warmup( executorFactory, multiplier, true, dumpQueryList );
            } else {
                System.err.println( "Unknown task: " + args.get( 0 ) );
            }
        } catch ( Throwable t ) {
            log.error( "Exception while executing Gavel!", t );
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
