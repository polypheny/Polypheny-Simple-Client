package org.polypheny.simpleclient.scenario.external;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public interface BenchmarkRunner {

    void setup( File outputDirectory ) throws IOException;

    void load( File outputDirectory ) throws IOException;

    void warmUp( File outputDirectory ) throws IOException;

    void run( File outputDirectory ) throws IOException;

    void analyze( Properties results, File outputDirectory ) throws IOException;
}
