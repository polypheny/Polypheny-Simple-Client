package org.polypheny.simpleclient.cli;


import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.sql.SQLException;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.PolyphenyDbJdbcExecutor.PolyphenyDbJdbcExecutorFactory;
import org.polypheny.simpleclient.main.Knn;


@Slf4j
@Command(name = "knn", description = "Mode for quick testing of Polypheny-DB using the KnnBench benchmark.")
public class KnnCommand implements CliRunnable {

    @SuppressWarnings("SpringAutowiredFieldsWarningInspection")
    @Inject
    private HelpOption<EasyCommand> help;

    @Arguments(description = "Task { schema | data | workload } and multiplier.")
    private List<String> args;


    @Option(name = { "-pdb", "--polyphenydb" }, title = "IP or Hostname", arity = 1, description = "IP or Hostname of the Polypheny-DB server (default: 127.0.0.1).")
    public static String polyphenyDbHost = "127.0.0.1";


    @Option(name = { "--writeCSV" }, arity = 0, description = "Write a CSV file containing execution times for all executed queries (default: false).")
    public boolean writeCsv = false;


    @Option(name = { "--queryList" }, arity = 0, description = "Dump all Gavel queries as SQL into a file (default: false).")
    public boolean dumpQueryList = false;


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

        ExecutorFactory executorFactory;
        executorFactory = new PolyphenyDbJdbcExecutorFactory( polyphenyDbHost, false );

        if ( args.get( 0 ).equalsIgnoreCase( "data" ) ) {
            Knn.data( executorFactory, multiplier, true );
        } else if ( args.get( 0 ).equalsIgnoreCase( "workload" ) ) {
            Knn.workload( executorFactory, multiplier, true, writeCsv, dumpQueryList );
        } else if ( args.get( 0 ).equalsIgnoreCase( "schema" ) ) {
            Knn.schema( executorFactory, true );
        } else if ( args.get( 0 ).equalsIgnoreCase( "warmup" ) ) {
            Knn.warmup( executorFactory, multiplier, true, dumpQueryList );
        } else {
            System.err.println( "Unknown task: " + args.get( 0 ) );
        }

        try {
            Thread.sleep( 2000 );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( "Unexpected interrupt", e );
        }

        return 0;
    }

}
