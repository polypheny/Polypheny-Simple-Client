package org.polypheny.simpleclient.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.AirlineModule;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.util.List;
import org.polypheny.simpleclient.main.ParquetExternalScenario;


@Command(
        name = "parquet-external",
        description = "Set up Parquet sources and run external Parquet benchmarks."
)
public class ParquetExternalCommand implements CliRunnable {

    @AirlineModule
    private HelpOption<ParquetExternalCommand> help;

    @Arguments( description = "Task { setup | workload } and optional benchmark name for workload." )
    private List<String> args;

    @Option(
            name = "--benchmark-host",
            arity = 1,
            description = "Polypheny host as reachable from the benchmark container."
    )
    private String host = "host.docker.internal";

    @Option(
            name = "--benchmark-port",
            arity = 1,
            description = "Polypheny Prism port."
    )
    private int port = 20590;

    @Option(name = "--setup-host", arity = 1, description = "Polypheny JDBC host as reachable from the Simple Client.")
    private String setupHost = "127.0.0.1";

    @Option(name = "--setup-port", arity = 1, description = "Polypheny Prism port used to configure sources.")
    private int setupPort = 20590;


    @Override
    public int run() {
        if ( args == null || args.isEmpty() || args.size() > 2 ) {
            throw new IllegalArgumentException( "Expected: parquet-external { setup | workload [benchmark] }" );
        }

        String task = args.getFirst();

        String benchmark = args.size() == 2 ? args.get( 1 ) : "aggregation-sql";

        if ( task.equalsIgnoreCase( "setup" ) ) {
            if ( args.size() != 1 ) {
                throw new IllegalArgumentException( "Expected: parquet-external setup" );
            }
            ParquetExternalScenario.setup( setupHost, setupPort );
        } else if ( task.equalsIgnoreCase( "workload" ) ) {
            ParquetExternalScenario.workload( host, port, benchmark );
        } else {
            throw new IllegalArgumentException( "Unknown task: " + task );
        }

        return 0;
    }
}
