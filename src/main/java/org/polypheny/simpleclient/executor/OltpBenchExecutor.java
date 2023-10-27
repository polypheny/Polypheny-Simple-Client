/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2022 The Polypheny Project
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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.AbstractConfig;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchConfig;


@Slf4j
public abstract class OltpBenchExecutor implements Executor {

    public static final String OLTPBENCH_RELEASE_URL;
    public static final String FILE_NAME;
    public static final String CLIENT_DIR;


    static {
        if ( SystemUtils.IS_OS_WINDOWS ) {
            OLTPBENCH_RELEASE_URL = "https://github.com/polypheny/OLTPBench/releases/download/v1.2.1/oltpbench-polypheny-1.2.1-jdk11-windows64.zip";
        } else if ( SystemUtils.IS_OS_LINUX ) {
            OLTPBENCH_RELEASE_URL = "https://github.com/polypheny/OLTPBench/releases/download/v1.2.1/oltpbench-polypheny-1.2.1-jdk11-linux64.zip";
        } else if ( SystemUtils.IS_OS_MAC ) {
            OLTPBENCH_RELEASE_URL = "https://github.com/polypheny/OLTPBench/releases/download/v1.2.1/oltpbench-polypheny-1.2.1-jdk11-mac64.zip";
        } else {
            throw new RuntimeException( "Unknown OS: " + SystemUtils.OS_NAME );
        }
        FILE_NAME = OLTPBENCH_RELEASE_URL.substring( OLTPBENCH_RELEASE_URL.lastIndexOf( '/' ) + 1 );
        CLIENT_DIR = System.getProperty( "user.home" ) + File.separator + ".polypheny" + File.separator + "client" + File.separator;
    }


    public OltpBenchExecutor() {
        downloadOltpBench();
    }


    @Override
    public void reset() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }

    @Override
    public abstract long executeQuery( Query query ) throws ExecutorException;


    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        throw new ExecutorException( "Unsupported Operation" );
    }


    @Override
    public void executeCommit() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeRollback() throws ExecutorException {
        throw new ExecutorException( "Unsupported operation" );
    }


    @Override
    public void closeConnection() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeInsertList( List<BatchableInsert> queryList, AbstractConfig config ) throws ExecutorException {
        throw new ExecutorException( "Unsupported operation" );
    }


    private void downloadOltpBench() {
        if ( !new File( CLIENT_DIR + FILE_NAME ).exists() ) {
            new File( CLIENT_DIR ).mkdirs();
            try {
                ReadableByteChannel readableByteChannel = Channels.newChannel( new URL( OLTPBENCH_RELEASE_URL ).openStream() );
                try ( FileOutputStream fileOutputStream = new FileOutputStream( CLIENT_DIR + FILE_NAME ) ) {
                    fileOutputStream.getChannel().transferFrom( readableByteChannel, 0, Long.MAX_VALUE );
                }
                try ( ZipFile zipFile = new ZipFile( CLIENT_DIR + FILE_NAME ) ) {
                    zipFile.extractAll( CLIENT_DIR );
                }
                File oltpBenchDir = new File( CLIENT_DIR + "oltpbench" );
                if ( oltpBenchDir.exists() ) {
                    FileUtils.deleteDirectory( oltpBenchDir );
                }
                new File( CLIENT_DIR + FILE_NAME.substring( 0, FILE_NAME.lastIndexOf( "." ) ) ).renameTo( oltpBenchDir );
            } catch ( IOException e ) {
                throw new RuntimeException( "Error while downloading OLTPbench", e );
            }
        }
    }


    private String writeConfigFile( AbstractOltpBenchConfig config, String fileName ) {
        try {
            String outputFilePath = CLIENT_DIR + fileName;
            FileWriter fileWriter = new FileWriter( CLIENT_DIR + fileName );
            BufferedWriter writer = new BufferedWriter( fileWriter );
            writer.write( getConfigXml( config ) );
            writer.close();
            return outputFilePath;
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }


    public void createSchema( AbstractOltpBenchConfig config ) throws ExecutorException {
        String configFilePath = writeConfigFile( config, config.scenario + ".xml" );

        ProcessBuilder builder = new ProcessBuilder();
        builder.command( "./bin/oltpbenchmark", "-b", config.scenario, "-c", configFilePath, "--create=true", "--load=false", "--execute=false" );
        builder.directory( new File( CLIENT_DIR + "oltpbench" ) );

        try {
            Process process = builder.start();
            StreamGobbler.startFor( process, true, null );
            int exitCode = process.waitFor();
        } catch ( IOException | InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }


    public void loadData( AbstractOltpBenchConfig config ) throws ExecutorException {
        String configFilePath = writeConfigFile( config, config.scenario + ".xml" );

        ProcessBuilder builder = new ProcessBuilder();
        builder.command( "./bin/oltpbenchmark", "-b", config.scenario, "-c", configFilePath, "--create=false", "--load=true", "--execute=false" );
        builder.directory( new File( CLIENT_DIR + "oltpbench" ) );

        try {
            Process process = builder.start();
            StreamGobbler.startFor( process, true, null );
            int exitCode = process.waitFor();
        } catch ( IOException | InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }


    public void executeWorkload( AbstractOltpBenchConfig config, File outputDir ) throws ExecutorException {
        String configFilePath = writeConfigFile( config, config.scenario + ".xml" );

        ProcessBuilder builder = new ProcessBuilder();
        builder.command( "./bin/oltpbenchmark", "-b", config.scenario, "-c", configFilePath, "--create=false", "--load=false", "--execute=true" );
        builder.directory( new File( CLIENT_DIR + "oltpbench" ) );

        try {
            FileWriter writer = new FileWriter( new File( outputDir, "oltpbench.log" ) );
            Process process = builder.start();
            StreamGobbler.startFor( process, true, writer );
            int exitCode = process.waitFor();
        } catch ( IOException | InterruptedException e ) {
            throw new RuntimeException( e );
        }

        new File( configFilePath )
                .renameTo( new File( outputDir, config.scenario + ".xml" ) );
        new File( CLIENT_DIR + "oltpbench" + File.separator + "results" + File.separator + "oltpbench.csv" )
                .renameTo( new File( outputDir, "oltpbench.csv" ) );
    }


    protected abstract String getConfigXml( AbstractOltpBenchConfig config );


    @Override
    public void flushCsvWriter() {
        // CSV writer is not supported with OLTPBench Executor
    }


    public abstract static class OltpBenchExecutorFactory extends ExecutorFactory {

        @Override
        public abstract OltpBenchExecutor createExecutorInstance();


        @Override
        public OltpBenchExecutor createExecutorInstance( CsvWriter csvWriter ) {
            if ( csvWriter == null ) {
                return  createExecutorInstance();
            } else {
                throw new RuntimeException("CSV writer is not supported with OltpBench!");
            }
        }


        // Allows to limit number of concurrent executor threads, 0 means no limit
        @Override
        public abstract int getMaxNumberOfThreads();

    }


    // From: https://gist.github.com/jmartisk/6535784
    public static class StreamGobbler extends Thread {

        private final InputStream inputStream;
        private final boolean printOutput;
        private final FileWriter writer;


        private StreamGobbler( final InputStream inputStream, boolean printOutput, FileWriter writer ) {
            this.inputStream = inputStream;
            this.printOutput = printOutput;
            this.writer = writer;
        }


        private StreamGobbler( final InputStream inputStream, FileWriter writer ) {
            this.inputStream = inputStream;
            printOutput = false;
            this.writer = writer;
        }


        static void startFor( final Process process ) {
            new StreamGobbler( process.getErrorStream(), null ).start();
            new StreamGobbler( process.getInputStream(), null ).start();
        }


        static void startFor( final Process process, boolean printOutput, FileWriter writer ) {
            new StreamGobbler( process.getErrorStream(), printOutput, writer ).start();
            new StreamGobbler( process.getInputStream(), printOutput, writer ).start();
        }


        private boolean isIgnoredLogLine( String logLine ) {
            return logLine.startsWith( "WARNING: An illegal reflective access operation has occurred" ) ||
                    logLine.startsWith( "WARNING: Illegal reflective access by com.sun.xml.bind.v2.runtime.reflect.opt.Injector" ) ||
                    logLine.startsWith( "WARNING: Please consider reporting this to the maintainers of com.sun.xml.bind.v2.runtime.reflect.opt.Injector" ) ||
                    logLine.startsWith( "WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations" ) ||
                    logLine.startsWith( "WARNING: All illegal access operations will be denied in a future release" );
        }


        @Override
        public void run() {
            try {
                final BufferedReader reader = new BufferedReader( new InputStreamReader( inputStream ) );
                String line;
                while ( (line = reader.readLine()) != null ) {
                    if ( printOutput ) {
                        if ( line.contains( "::" ) || (!line.contains( ") DEBUG - " ) && !line.contains( ") INFO  - " ) && !isIgnoredLogLine( line )) ) {
                            log.info( "OLTPBench> " + line );
                        }
                        if ( writer != null ) {
                            writer.append( line ).append( System.lineSeparator() );
                            writer.flush();
                        }
                    }
                }
            } catch ( IOException ioe ) {
                ioe.printStackTrace();
            }
        }

    }

}
