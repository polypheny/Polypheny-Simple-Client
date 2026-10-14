/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-12.10.26, 09:59 The Polypheny Project
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.polypheny.simpleclient.scenario.external;

import org.polypheny.simpleclient.main.DockerLauncher;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public abstract class ExternalBenchmarkRunner {

    private final String benchmarkName;
    private final String imageName;
    private final String network;


    protected ExternalBenchmarkRunner( String benchmarkName, String imageName, String network ) {
        this.benchmarkName = benchmarkName;
        this.imageName = imageName;
        this.network = network;
    }


    public final void run( File outputDirectory ) {
        try {
            Files.createDirectories( outputDirectory.toPath() );
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not create results directory: " + outputDirectory, e );
        }

        File logFile = new File( outputDirectory, "run.log" );

        List<String> command = buildCommand();
        Map<File, String> volumes = buildVolumes( outputDirectory );

        prepareRun( outputDirectory );

        DockerLauncher.runJob(
                benchmarkName + UUID.randomUUID(),
                imageName,
                Map.of(),
                network,
                command,
                logFile,
                volumes
        );

    }
    public abstract void analyze( Properties results, File outputDirectory );
    protected abstract void prepareRun( File outputDirectory );
    protected abstract List<String> buildCommand();
    protected Map<File, String> buildVolumes( File outputDirectory ) { return Map.of(); }
}




