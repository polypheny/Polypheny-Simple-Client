package org.polypheny.simpleclient.cli;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.polypheny.simpleclient.main.Ch2ExternalScenario;

@Command(name = "ch2-external", description = "External CH2: one config for setup, loading and workload execution.")
public class Ch2ExternalCommand implements CliRunnable {

    @Arguments(description = "Task: setup | data | warmup | workload | all")
    private List<String> args;

    @Option(name = "--config", arity = 1, description = "Experiment properties file")
    public String configFile = "docker/ch2/experiment.properties";

    @Option(name = "--output", arity = 1, description = "Result directory")
    public String outputDirectory = "results/ch2";

    @Option(name = "--managed", arity = 0, description = "Build images and start a temporary Docker environment for all")
    public boolean managed;

    @Override
    public int run() throws IOException {
        if ( args == null || args.size() != 1 || outputDirectory == null ) {
            throw new IllegalArgumentException( "Expected: ch2-external {setup|data|warmup|workload|all} --config FILE --output DIR" );
        }
        String task = args.getFirst().toLowerCase( Locale.ROOT );
        if ( managed && !task.equals( "all" ) ) {
            throw new IllegalArgumentException( "--managed requires all" );
        }
        if ( managed ) {
            Ch2ExternalScenario.executeManaged( Path.of( configFile ), Path.of( outputDirectory ) );
        } else {
            Ch2ExternalScenario.execute( Path.of( configFile ), task, Path.of( outputDirectory ) );
        }
        return 0;
    }
}
