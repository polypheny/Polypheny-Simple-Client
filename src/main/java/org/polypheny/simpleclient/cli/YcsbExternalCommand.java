package org.polypheny.simpleclient.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.AirlineModule;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.util.List;
import org.polypheny.simpleclient.main.YcsbExternalScenario;


@Command(name = "ycsb-external", description = "Run the external YCSB benchmark against Polypheny.")

public class YcsbExternalCommand implements CliRunnable {

    @AirlineModule
    private HelpOption<YcsbExternalCommand> help;

    @Arguments( description = "Task { schema | data | workload } and multiplier." )
    private List<String> args;

    @Option( name = { "-pdb", "--polyphenydb" }, title = "Host and port", arity = 1, description = "Polypheny JDBC address as reachable from the Simple Client." )
    public String polyphenyDbHost = "127.0.0.1:20590";

    @Option( name = { "--ycsb-host" }, title = "Host and port", arity = 1, description = "Polypheny JDBC address as reachable from the YCSB container.")
    public String ycsbHost = "host.docker.internal:20590";


    @Override
    public int run() {
        if ( args == null || args.isEmpty() || args.size() > 2 ) {
            throw new IllegalArgumentException( "Expected: ycsb-external { schema | data | workload } [multiplier]" );
        }

        String task = args.getFirst();

        int multiplier = args.size() == 2 ? Integer.parseInt( args.get( 1 ) ) : 1;

        if ( multiplier < 1 ) {
            throw new IllegalArgumentException( "Multiplier must be greater than 0" );
        }

        if ( task.equalsIgnoreCase( "data" ) ) {
            YcsbExternalScenario.data( polyphenyDbHost, ycsbHost, multiplier );
        } else if ( task.equalsIgnoreCase( "workload" ) ) {
            YcsbExternalScenario.workload( polyphenyDbHost, ycsbHost, multiplier );
        } else if ( task.equalsIgnoreCase( "schema" ) ) {
            YcsbExternalScenario.schema( polyphenyDbHost, ycsbHost );
        } else {
            throw new IllegalArgumentException( "Unknown task: " + task );
        }

        return 0;
    }

}
