package org.polypheny.simpleclient.main;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.polypheny.simpleclient.main.ExternalBenchmarkPhases.Phase;
import org.polypheny.simpleclient.scenario.ch2.Ch2Config;
import org.polypheny.simpleclient.scenario.ch2.Ch2Runner;

public class Ch2ExternalScenario {
    public static void execute( Path configuration, String task, Path outputRoot ) throws IOException {
        execute( new Ch2Config( configuration ), task, outputRoot );
    }

    public static void execute( Ch2Config config, String task, Path outputRoot ) throws IOException {
        execute( config, ExternalBenchmarkPhases.forTask( "CH2", task, config.warmupTime > 0 ), outputRoot );
    }

    public static void executeManaged( Path configuration, Path outputRoot ) throws IOException {
        new Ch2BenchmarkJob( configuration, null ).runLocal( outputRoot );
    }

    static Properties execute( Ch2Config config, List<Phase> phases, Path outputRoot ) throws IOException {
        return ExternalBenchmarkPhases.execute( "CH2", phases, outputRoot, List.of( "polypheny" ), config.properties,
                target -> new Ch2Runner( config ), "" );
    }
}
