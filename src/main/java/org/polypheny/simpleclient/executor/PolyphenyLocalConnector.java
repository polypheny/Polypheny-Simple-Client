/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-8/8/24, 2:55 PM The Polypheny Project
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

package org.polypheny.simpleclient.executor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import kong.unirest.core.HttpResponse;
import kong.unirest.core.Unirest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.control.client.LogHandler;

@Slf4j
public class PolyphenyLocalConnector implements PolyphenyConnector {

    private PolyphenyConnectorConfig config;
    private File jar;
    private Process process = null;
    private LogHandler handler;
    private String buildServer;


    public PolyphenyLocalConnector( String build, LogHandler logHandler ) throws URISyntaxException {
        File home = new File( System.getProperty( "user.home" ) );
        jar = new File( home, "polypheny.jar" );
        handler = logHandler;
        buildServer = build;
    }


    public void stopPolypheny() {
//        if ( process != null ) {
//            process.destroyForcibly();
//            while ( true ) {
//                try {
//                    process.waitFor();
//                    break;
//                } catch ( InterruptedException ignored ) {
//                }
//            }
//            process = null;
//        }
    }


    public void startPolypheny() {
        try {
            List<String> args = new ArrayList<>();
            args.add( "java" );
            args.add( "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:5006" );
            args.add( String.format( "-Xmx%dG", config.heapSizeGb() ) );
            args.add( "-Djava.net.preferIPv4Stack=true" );
            args.add( "-jar" );
            args.add( jar.getAbsolutePath() );
            if ( config.resetCatalog() ) {
                args.add( "-resetCatalog" );
            }
            if ( config.memoryCatalog() ) {
                args.add( "-memoryCatalog" );
            }
            args.add( "-mode" );
            args.add( "benchmark" );
            process = new ProcessBuilder( args ).start();
            new Thread( new CopyStream( process.getInputStream() ) ).start();
            new Thread( new CopyStream( process.getErrorStream() ) ).start();
        } catch ( IOException e ) {
            throw new RuntimeException( "Failed to start Polypheny: ", e );
        }
    }


    public void purgePolyphenyFolder() {
    }


    public void updatePolypheny() {
        // Download JAR
        downloadJar( buildServer, config.branchDB(), config.commit(), jar );
    }


    private static void downloadJar( String buildServer, String branch, String commit, File jar ) {
        String newCommitId;
        if ( commit == null || commit.isEmpty() ) {
            log.warn( "Getting commit id for branch {} from GitHub", branch );

            newCommitId = GithubApi.getCommit( branch ).sha();
            log.warn( "Fetching commit {} on branch {} from {}", newCommitId, branch, buildServer );
        } else {
            log.warn( "Using commit id from CDL {}: {}", branch, commit );
            newCommitId = commit;
        }

        HttpResponse<byte[]> req = Unirest.get( String.format( "%s/jar?commit=%s&branch=%s", buildServer, newCommitId, branch ) )
                .requestTimeout( 30 * 60 * 1000 )
                .asBytes();

        if ( req.getStatus() != 200 ) {
            throw new RuntimeException( "HTTP response code " + req.getStatus() );
        }

        try ( OutputStream fw = new FileOutputStream( jar ) ) {
            fw.write( req.getBody() );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

        log.warn( "Fetching commit {} on branch {} from {} ... finished.", newCommitId, branch, buildServer );
        log.warn( "Using Polypheny-DB commit {}", newCommitId );
    }


    public void setConfig( PolyphenyConnectorConfig config ) {
        if ( config.buildUI() ) {
            throw new RuntimeException( "Build UI is not supported for PolyphenyLocalConnector" );
        }
        this.config = config;
    }


    public String getConfig() {
        throw new NotImplementedException();
    }


    public String getVersion() {
        return "";
    }


    public int checkForAnyRunningPolyphenyInstances() {
        return 0;
    }


    private class CopyStream implements Runnable {

        private final BufferedReader in;


        CopyStream( InputStream in ) {
            this.in = new BufferedReader( new InputStreamReader( in ) );
        }


        public void run() {
            try {
                in.lines().forEach( handler::handleLogMessage );
            } catch ( UncheckedIOException e ) {
                if ( e.getCause().getMessage().equals( "Stream closed" ) ) {
                    return;
                }
                throw e;
            }
        }

    }

}
