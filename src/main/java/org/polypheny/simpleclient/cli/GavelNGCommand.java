/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 The Polypheny Project
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbMongoQlExecutor.PolyphenyDbMongoQlExecutorFactory;
import org.polypheny.simpleclient.main.GavelExScenario;


@Slf4j
@Command(name = "gavelEx", description = "Mode for quick testing of Polypheny-DB using the Gavel benchmark.")
public class GavelNGCommand implements CliRunnable {

    @SuppressWarnings("SpringAutowiredFieldsWarningInspection")
    @Inject
    private HelpOption<GavelNGCommand> help;

    @Arguments(description = "Task { schema | data | workload | warmup } and multiplier and { view | materialized }.")
    private List<String> args;

    @Option(name = { "-pdb", "--polyphenydb" }, title = "IP or Hostname", arity = 1, description = "IP or Hostname of  Polypheny-DB (default: 127.0.0.1).")
    public static String polyphenyDbHost = "127.0.0.1";

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

        QueryMode queryMode = QueryMode.TABLE;
        int multiplier = 1;
        if ( args.size() > 1 ) {
            multiplier = Integer.parseInt( args.get( 1 ) );
            if ( multiplier < 1 ) {
                System.err.println( "Multiplier needs to be a integer > 0!" );
                System.exit( 1 );
            }
            if ( args.size() > 2 ) {
                if ( args.get( 2 ).equalsIgnoreCase( "view" ) ) {
                    queryMode = QueryMode.VIEW;
                } else if ( args.get( 2 ).equalsIgnoreCase( "materialized" ) ) {
                    queryMode = QueryMode.MATERIALIZED;
                }
            }
        }

        ExecutorFactory executorFactoryMONGODB = new PolyphenyDbMongoQlExecutorFactory( polyphenyDbHost );
        ExecutorFactory executorFactoryHSQLDB = new PolyphenyDbJdbcExecutorFactory( polyphenyDbHost, true );

        try {
            if ( args.get( 0 ).equalsIgnoreCase( "data" ) ) {
                GavelExScenario.data( executorFactoryHSQLDB, executorFactoryMONGODB, multiplier, true, queryMode );
            } else if ( args.get( 0 ).equalsIgnoreCase( "workload" ) ) {
                GavelExScenario.workload( executorFactoryHSQLDB, executorFactoryMONGODB, multiplier, true, writeCsv, dumpQueryList, queryMode );
            } else if ( args.get( 0 ).equalsIgnoreCase( "schema" ) ) {
                List<String> dataStores = Arrays.asList( "hsqldb", "mongodb" );
                deploySelectedStore( executorFactoryHSQLDB, dataStores );
                GavelExScenario.schema( executorFactoryHSQLDB, executorFactoryMONGODB, true, queryMode );
            } else if ( args.get( 0 ).equalsIgnoreCase( "warmup" ) ) {
                GavelExScenario.warmup( executorFactoryHSQLDB, executorFactoryMONGODB, multiplier, true, dumpQueryList, queryMode );
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


    public Map<String, String> deploySelectedStore( ExecutorFactory executorFactory, List<String> dataStore ) {

        PolyphenyDbExecutor executor = (PolyphenyDbExecutor) executorFactory.createExecutorInstance();
        try {
            // Remove hsqldb store
            executor.dropStore( "hsqldb" );
            // Deploy stores
            for ( String store : dataStore ) {
                switch ( store ) {
                    case "hsqldb":
                        executor.deployHsqldb();
                        break;
                    case "postgres":
                        executor.deployPostgres( true );
                        break;
                    case "monetdb":
                        executor.deployMonetDb( true );
                        break;
                    case "cassandra":
                        executor.deployCassandra( true );
                        break;
                    case "file":
                        executor.deployFileStore();
                        break;
                    case "cottontail":
                        executor.deployCottontail();
                        break;
                    case "mongodb":
                        executor.deployMongoDb();
                        break;
                    default:
                        throw new RuntimeException( "Unknown data store: " + store );
                }
            }
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while configuring stores", e );
        } finally {
            try {
                executor.closeConnection();
            } catch ( ExecutorException e ) {
                log.error( "Exception while closing connection", e );
            }
        }

        return executor.getDataStoreNames();
    }


}
