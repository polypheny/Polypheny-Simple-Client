package org.polypheny.simpleclient.cli;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.polypheny.simpleclient.main.YcsbExternalScenario;

@Command(name = "ycsb-external", description = "External YCSB: one config for setup, loading and comparison.")
public class YcsbExternalCommand implements CliRunnable {

    @Arguments(description = "Task: setup | data | warmup | workload | all")
    private List<String> args;

    @Option(name = "--config", arity = 1, description = "Experiment properties file")
    public String configFile = "docker/ycsb/experiment.properties";

    @Option(name = "--output", arity = 1, description = "Result directory (required)")
    public String outputDirectory;

    @Option(name = "--managed", arity = 0, description = "Build images and start a temporary Docker environment for all")
    public boolean managed;

    @Override
    public int run() throws IOException {
        if ( args == null || args.size() != 1 || outputDirectory == null ) {
            throw new IllegalArgumentException( "Expected: ycsb-external {setup|data|warmup|workload|all} --config FILE --output DIR" );
        }
        String task = args.getFirst();
        if ( managed && !task.equals( "all" ) ) {
            throw new IllegalArgumentException( "--managed requires all" );
        }
        if ( managed ) {
            YcsbExternalScenario.executeManaged( Path.of( configFile ), Path.of( outputDirectory ) );
        } else {
            YcsbExternalScenario.execute( Path.of( configFile ), task, Path.of( outputDirectory ) );
        }
        return 0;
    }
}
