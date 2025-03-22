/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/21/25, 1:02 PM The Polypheny Project
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

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.sql.SQLException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.main.LockingBenchScenario;

@Slf4j
@Command(name = "locking", description = "Mode for quick testing of Polypheny-DB using the locking benchmark.")
public class LockingBenchCommand implements CliRunnable {

    @Arguments(description = "Task { schema | data | workload | warmup }.")
    private List<String> args;

    @Option(name = { "-pdb", "--polyphenydb" }, title = "IP or Hostname", arity = 1, description = "IP or Hostname of  Polypheny-DB (default: 127.0.0.1).")
    public static String polyphenyDbHost = "127.0.0.1";

    @Option(name = { "-s", "--sessions" }, title = "Numbers of sessions", arity = 1, description = "The number of session accessing the database simultaneously (default 5).")
    public int sessionCount = 5;

    @Option(name = { "-n", "--namespaces" }, title = "Numbers of namespaces", arity = 1, description = "The number of namespaces the entities are distributed over (default 2).")
    public int namespaceCount = 2;

    @Option(name = { "-e", "--entities" }, title = "Numbers of entities", arity = 1, description = "The number of entities to be used created in each namespace (default 5).")
    public int entityCount = 5;

    @Option(name = { "-r", "--rwRatio" }, title = "Read Write Ratio", arity = 1, description = "The ratio between read and modify/write queries from 0 to 1 (default 0.5).")
    public double readWriteRatio = 0.5;


    @Override
    public int run() throws SQLException {
        if ( args == null || args.isEmpty() ) {
            System.err.println( "Missing task" );
            System.exit( 1 );
        }

        ExecutorFactory executorFactory = new PolyphenyDbJdbcExecutorFactory( polyphenyDbHost, false );

        try {
            if ( args.getFirst().equalsIgnoreCase( "schema" ) ) {
                LockingBenchScenario.schema( executorFactory, sessionCount, namespaceCount, entityCount, readWriteRatio );
            } else if ( args.getFirst().equalsIgnoreCase( "data" ) ) {
                LockingBenchScenario.data( executorFactory, sessionCount, namespaceCount, entityCount, readWriteRatio );
            } else if ( args.getFirst().equalsIgnoreCase( "workload" ) ) {
                LockingBenchScenario.workload( executorFactory, sessionCount, namespaceCount, entityCount, readWriteRatio );
            } else if ( args.getFirst().equalsIgnoreCase( "warmup" ) ) {
                LockingBenchScenario.warmup( executorFactory, sessionCount, namespaceCount, entityCount, readWriteRatio );
            } else {
                System.err.println( "Unknown task: " + args.get( 0 ) );
            }
        } catch ( Throwable t ) {
            log.error( "Exception while executing LockingBench!", t );
            System.exit( 1 );
        }

        return 0;
    }


}
