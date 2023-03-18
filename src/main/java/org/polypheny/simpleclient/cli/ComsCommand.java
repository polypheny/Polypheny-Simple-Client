/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/15/23, 3:06 PM The Polypheny Project
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
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.sql.SQLException;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.MultiExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbMultiExecutorFactory;
import org.polypheny.simpleclient.executor.PostgresExecutor;
import org.polypheny.simpleclient.executor.SurrealDBExecutor.SurrealDBExecutorFactory;
import org.polypheny.simpleclient.main.ComsScenario;
import org.polypheny.simpleclient.main.CsvWriter;

@Slf4j
@Command(name = "coms", description = "Mode for testing the Coms-Benchmark.")
public class ComsCommand implements CliRunnable {

    @SuppressWarnings("SpringAutowiredFieldsWarningInspection")
    @Inject
    private HelpOption<AbstractOltpBenchCommand> help;

    @Arguments(description = "Task { schema | data | workload } and multiplier.")
    private List<String> args;

    @Option(name = { "--mode" }, arity = 0, description = "Which stack to use, either a native stack(Neo4j, MongoDB, PostgreSQL), Polypheny or SurrealDB (default:Polypheny).")
    public static Mode mode = Mode.POLYPHENY;


    @Option(name = { "-p", "--polypheny" }, title = "IP or Hostname", arity = 1, description = "IP or Hostname of the server (default: 127.0.0.1).")
    public String polyphenyDbHost = "127.0.0.1";

    @Option(name = { "-s", "--surreal" }, title = "IP or Hostname + Port", arity = 1, description = "IP or Hostname of the SurrealDB server (default: 127.0.0.1).")
    public String surrealHost = "127.0.0.1:8000";
    @Option(name = { "-n", "--neo4j" }, title = "IP or Hostname + Port", arity = 1, description = "IP or Hostname of the Neo4j server (default: 127.0.0.1).")
    public String neo4j = "127.0.0.1";
    @Option(name = { "-m", "--mongo" }, title = "IP or Hostname + Port", arity = 1, description = "IP or Hostname of the MongoDB server (default: 127.0.0.1).")
    public String mongoDB = "127.0.0.1";

    @Option(name = { "-po", "--postgres" }, title = "IP or Hostname + Port", arity = 1, description = "IP or Hostname of the PostgreSQL server (default: 127.0.0.1).")
    public String postgres = "127.0.0.1";

    @Option(name = { "--writeCSV" }, arity = 0, description = "Write a CSV file containing execution times for all executed queries (default: false).")
    public boolean writeCsv = false;


    @Override
    public int run() throws SQLException {
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

        //// Define executorFactory, depending on cli parameter
        ExecutorFactory executorFactory;

        switch ( mode ) {

            case POLYPHENY:
                executorFactory = new PolyphenyDbMultiExecutorFactory( polyphenyDbHost, "coms" );
                break;
            case NATIVE:
                executorFactory = new MultiExecutorFactory(
                        new PostgresExecutor.PostgresExecutorFactory( postgres, false ),
                        new NeoExecutorFactory( neo4j ),
                        new MongoQlExecutorFactory( mongoDB ) );
                break;
            case SURREAL:
                executorFactory = new SurrealDBExecutorFactory( surrealHost );
                break;
            default:
                throw new IllegalArgumentException();
        }

        try {
            if ( args.get( 0 ).equalsIgnoreCase( "data" ) ) {
                ComsScenario.data( executorFactory, mode, multiplier );
            } else if ( args.get( 0 ).equalsIgnoreCase( "workload" ) ) {
                ComsScenario.workload( executorFactory, mode, multiplier, writeCsv );
            } else if ( args.get( 0 ).equalsIgnoreCase( "schema" ) ) {
                ComsScenario.schema( executorFactory, mode );
            } else {
                System.err.println( "Unknown task: " + args.get( 0 ) );
            }
        } catch ( Throwable t ) {
            log.error( "Exception while executing Coms!", t );
            System.exit( 1 );
        }

        try {
            Thread.sleep( 2000 );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( "Unexpected interrupt", e );
        }

        return 0;
    }


    public class NeoExecutorFactory extends ExecutorFactory {

        public NeoExecutorFactory( String host ) {
        }


        @Override
        public Executor createExecutorInstance( CsvWriter csvWriter ) {
            return null;
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }


    public static class MongoQlExecutorFactory extends ExecutorFactory {

        public MongoQlExecutorFactory( String host ) {
        }


        @Override
        public Executor createExecutorInstance( CsvWriter csvWriter ) {
            return null;
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }

}
