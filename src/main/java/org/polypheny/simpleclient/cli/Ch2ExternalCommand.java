package org.polypheny.simpleclient.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.AirlineModule;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import java.util.List;
import org.polypheny.simpleclient.main.Ch2ExternalScenario;


@Command(
        name = "ch2-external",
        description = "Run the external CH2 benchmark against Polypheny."
)
public class Ch2ExternalCommand implements CliRunnable {

    @AirlineModule
    private HelpOption<Ch2ExternalCommand> help;

    @Arguments( description = "Task { data | workload }." )
    private List<String> args;


    @Override
    public int run() {
        if ( args == null || args.size() != 1 ) {
            throw new IllegalArgumentException( "Expected: ch2-external { data | workload }" );
        }

        String task = args.getFirst().toLowerCase();

        if ( task.equals( "data" ) ) {
            Ch2ExternalScenario.data();
        } else if ( task.equals( "workload" ) ) {
            Ch2ExternalScenario.workload();
        } else {
            throw new IllegalArgumentException( "Unknown CH2 task: " + task );
        }

        return 0;
    }
}
