/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2024 The Polypheny Project
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

package org.polypheny.simpleclient.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DockerLauncher {

    public static String launch( String containerName, String imageName, Map<String, String> environment, List<Integer> ports, Supplier<Boolean> up ) {
        remove( containerName );

        try {
            List<String> args = new ArrayList<>( List.of( "docker", "run", "-d", "--name", containerName ) );
            environment.forEach( ( k, v ) -> args.addAll( List.of( "-e", String.format( "%s=%s", k, v ) ) ) );
            ports.forEach( p -> args.addAll( List.of( "-p", String.format( "127.0.0.1:%d:%d", p, p ) ) ) );
            args.add( imageName );
            log.warn( "Launching a Docker container with \"{}\"", String.join( " ", args ) );
            Process p = new ProcessBuilder( args ).start();
            if ( p.waitFor() != 0 ) {
                throw new RuntimeException( "Failed to execute Docker command" );
            }
            long now = System.currentTimeMillis();
            while ( !up.get() && System.currentTimeMillis() - now < 60000 ) {
                Thread.sleep( 100 );
            }
            if ( !up.get() ) {
                throw new RuntimeException( "Failed to start Docker container" );
            }
        } catch ( IOException | InterruptedException e ) {
            throw new RuntimeException( e );
        }

        return containerName;
    }


    public static void remove( String containerName ) {
        try {
            Process p = new ProcessBuilder( "docker", "rm", "-f", containerName ).start();
            p.waitFor();
        } catch ( IOException | InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }

    public static void runJob( String containerName, String imageName, Map<String, String> environment, String network, List<String> command, File logFile ) {
        runJob( containerName, imageName, environment, network, command, logFile, Map.of() );
    }


    public static void runJob( String containerName, String imageName, Map<String, String> environment, String network, List<String> command, File logFile, Map<File, String> volumes ) {

        try {
            Files.createDirectories( logFile.toPath().toAbsolutePath().getParent() );

            List<String> args = new ArrayList<>( List.of( "docker", "run", "--rm", "--name", containerName ) );

            environment.forEach( ( k, v ) -> args.addAll( List.of( "-e", String.format( "%s=%s", k, v ) ) ) );

            args.addAll( List.of( "--network", network ) );

            for ( Map.Entry<File, String> volume : volumes.entrySet() ) {
                if ( !volume.getKey().isDirectory() ) {
                    throw new IllegalArgumentException( "Mount directory does not exist: " + volume.getKey().getAbsolutePath() );
                }

                args.add( "--mount" );
                args.add( "type=bind,source=" + volume.getKey().getAbsolutePath() + ",target=" + volume.getValue() );
            }

            args.add( imageName );
            args.addAll( command );

            log.info( "Running Docker job {}", containerName );

            Process process = new ProcessBuilder( args ).redirectErrorStream( true ).redirectOutput( logFile ).start();

            int exitCode = process.waitFor();

            if ( exitCode != 0 ) {
                throw new RuntimeException( "Docker job failed with exit code " + exitCode + ". See log: " + logFile.getAbsolutePath() );
            }
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException( "Docker job interrupted: " + containerName, e );
        } catch ( IOException e ) {
            throw new IllegalStateException( "Could not run Docker job: " + containerName, e );
        }
    }
}
