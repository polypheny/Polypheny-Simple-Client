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
import com.github.rvesse.airline.annotations.Option;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.OltpBenchPolyphenyDbExecutor.OltpBenchPolyphenyDbExecutorFactory;


@Slf4j
public abstract class AbstractOltpBenchCommand implements CliRunnable {

    @AirlineModule
    private HelpOption<AbstractOltpBenchCommand> help;

    @Arguments(description = "Task { schema | data | workload } and multiplier.")
    private List<String> args;


    @Option(name = { "-pdb", "--polyphenydb" }, title = "IP or Hostname", arity = 1, description = "IP or Hostname of the Polypheny-DB server (default: 127.0.0.1).")
    public String polyphenyDbHost = "127.0.0.1";


    @Override
    public int run() throws SQLException {
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
        executorFactory = new OltpBenchPolyphenyDbExecutorFactory( polyphenyDbHost ) {
        };

        try {
            if ( args.getFirst().equalsIgnoreCase( "data" ) ) {
                data( executorFactory, multiplier );
            } else if ( args.getFirst().equalsIgnoreCase( "workload" ) ) {
                workload( executorFactory, multiplier );
            } else if ( args.getFirst().equalsIgnoreCase( "schema" ) ) {
                schema( executorFactory );
            } else {
                System.err.println( "Unknown task: " + args.getFirst() );
            }
        } catch ( Throwable t ) {
            log.error( "Exception while executing OltpBench!", t );
            System.exit( 1 );
        }

        try {
            Thread.sleep( 2000 );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( "Unexpected interrupt", e );
        }

        return 0;
    }


    protected abstract void schema( ExecutorFactory executorFactory );


    protected abstract void data( ExecutorFactory executorFactory, int multiplier );


    protected abstract void workload( ExecutorFactory executorFactory, int multiplier );


    protected Properties getProperties( String fileName ) {
        Properties props = new Properties();
        try {
            props.load( Objects.requireNonNull( ClassLoader.getSystemResourceAsStream( "org/polypheny/simpleclient/scenario/oltpbench/" + fileName ) ) );
        } catch ( IOException e ) {
            log.error( "Exception while reading properties file", e );
        }
        return props;
    }

}
