package org.polypheny.simpleclient.main;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.polypheny.simpleclient.main.ExternalBenchmarkPhases.Phase;
import org.polypheny.simpleclient.scenario.ycsb.YcsbConfig;
import org.polypheny.simpleclient.scenario.ycsb.YcsbRunner;

public class YcsbExternalScenario {
    public static void execute( Path configuration, String task, Path outputRoot ) throws IOException {
        execute( new YcsbConfig( configuration ), task, outputRoot );
    }

    public static void execute( YcsbConfig config, String task, Path outputRoot ) throws IOException {
        execute( config, ExternalBenchmarkPhases.forTask( "YCSB", task, config.warmupTime > 0 ), outputRoot );
    }

    public static void executeManaged( Path configuration, Path outputRoot ) throws IOException {
        new YcsbBenchmarkJob( configuration, null ).runLocal( outputRoot );
    }

    static Properties execute( YcsbConfig config, List<Phase> phases, Path outputRoot ) throws IOException {
        return ExternalBenchmarkPhases.execute( "YCSB", phases, outputRoot, config.targets, config.properties,
                target -> new YcsbRunner( config, target ), "ycsb." );
    }
}
