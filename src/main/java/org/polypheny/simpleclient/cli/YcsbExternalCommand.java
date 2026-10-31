package org.polypheny.simpleclient.cli;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.nio.file.Path;
import java.util.List;
import org.polypheny.simpleclient.main.YcsbExternalScenario;

@Command(name = "ycsb-external", description = "External YCSB: one config for setup, loading and comparison.")
public class YcsbExternalCommand implements CliRunnable {

    @Arguments(description = "Task: setup | data | workload | all")
    private List<String> args;

    @Option(name = "--config", arity = 1, description = "Experiment properties file")
    public String configFile = "docker/ycsb/experiment.properties";

    @Option(name = "--output", arity = 1, description = "Result directory (required)")
    public String outputDirectory;

    @Override
    public int run() {
        if ( args == null || args.size() != 1 || outputDirectory == null ) {
            throw new IllegalArgumentException( "Expected: ycsb-external {setup|data|workload|all} --config FILE --output DIR" );
        }
        try {
            YcsbExternalScenario.execute( Path.of( configFile ), args.getFirst(), Path.of( outputDirectory ) );
        } catch ( java.io.IOException e ) {
            throw new IllegalStateException( "Could not execute YCSB experiment", e );
        }
        return 0;
    }
}
