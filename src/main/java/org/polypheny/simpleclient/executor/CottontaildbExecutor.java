package org.polypheny.simpleclient.executor;

import com.google.gson.JsonObject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import kong.unirest.UnirestException;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.AbstractConfig;
import org.vitrivr.cottontail.CottontailKt;
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer;


@Slf4j
public class CottontaildbExecutor implements Executor {

    private final CsvWriter csvWriter;


    public CottontaildbExecutor( CsvWriter csvWriter ) {
        super();
        this.csvWriter = csvWriter;

        // TODO JS: Connect to cottontaildb (is already running in embedded mode when this constructor is called)
    }


    @Override
    public void reset() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        long time;
        if ( query.getCottontail() != null ) {
            // TODO JS: Build query
            try {
                long start = System.nanoTime();
                // TODO JS: execute query
                time = System.nanoTime() - start;
                if ( csvWriter != null ) {
                    csvWriter.appendToCsv( null/*TODO JS: The query*/, time );
                }
            } catch ( UnirestException e ) {
                throw new ExecutorException( e );
            }
        } else {
            throw new RuntimeException( "There is no Cottontail GRPC message defined for this query!" );
        }

        return time;
    }


    // TODO JS: You can also remove this method in case it is not required for the knnBench benchmark
    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        if ( query.getRest() != null ) {
            // TODO JS: Build query
            try {
                long start = System.nanoTime();
                // TODO: execute query
                if ( csvWriter != null ) {
                    csvWriter.appendToCsv( null/*TODO JS: The query*/, System.nanoTime() - start );
                }
                // TODO JS: Get result of a count query
                return 0;
            } catch ( UnirestException e ) {
                throw new ExecutorException( e );
            }
        } else {
            throw new RuntimeException( "There is no Cottontail GRPC message defined for this query!" );
        }
    }


    @Override
    public void executeCommit() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeRollback() throws ExecutorException {
        log.error( "Unsupported operation: Rollback" );
    }


    @Override
    public void closeConnection() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeInsertList( List<BatchableInsert> batchList, AbstractConfig config ) throws ExecutorException {
        String currentTable = null;
        List<JsonObject> rows = new ArrayList<>();
        for ( BatchableInsert query : batchList ) {
            if ( currentTable == null ) {
                currentTable = query.getTable();
            }

            if ( currentTable.equals( query.getTable() ) ) {
                rows.add( Objects.requireNonNull( query.getRestRowExpression() ) );
            } else {
                throw new RuntimeException( "Different tables in multi-inserts. This should not happen!" );
            }
        }
        if ( rows.size() > 0 ) {
            executeQuery( new RawQuery( null, Query.buildRestInsert( currentTable, rows ), false ) );
        }
    }


    @Override
    public void flushCsvWriter() {
        if ( csvWriter != null ) {
            try {
                csvWriter.flush();
            } catch ( IOException e ) {
                log.warn( "Exception while flushing csv writer", e );
            }
        }
    }


    public static class CottontailExecutorFactory extends ExecutorFactory {

        public CottontailExecutorFactory() {
        }


        @Override
        public CottontaildbExecutor createExecutorInstance( CsvWriter csvWriter ) {
            return new CottontaildbExecutor( csvWriter );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }


    public static class CottontailInstance extends DatabaseInstance {

        private final File folder;
        private transient final CottontailGrpcServer embeddedServer;


        public CottontailInstance() {
            File clientFolder = new File( new File( System.getProperty( "user.home" ), ".polypheny" ), "client" );
            folder = new File( clientFolder, "cottontail" );

            if ( !folder.exists() ) {
                if ( !folder.mkdirs() ) {
                    throw new RuntimeException( "Could not create data directory" );
                }
            }

            File configFile = new File( folder, "config.json" );
            if ( !configFile.exists() ) {
                try {
                    configFile.createNewFile();
                    File dataFolder = new File( folder, "data" );
                    FileWriter fileWriter = new FileWriter( configFile );
                    fileWriter.write( "{" );
                    fileWriter.write( "\"root\": \"" + dataFolder.getAbsolutePath() + "\"," );
                    fileWriter.write( "\"mapdb\": {" );
                    fileWriter.write( "\"enableMmap\": true," );
                    fileWriter.write( "\"forceUnmap\": true," );
                    fileWriter.write( "\"pageShift\": 22" );
                    fileWriter.write( "} }" );
                    fileWriter.close();
                } catch ( IOException e ) {
                    throw new RuntimeException( e );
                }
            }
            this.embeddedServer = CottontailKt.embedded( configFile.getAbsolutePath() );
            this.embeddedServer.start();
        }


        @Override
        public void tearDown() {
            recursiveDeleteFolder( folder );
            embeddedServer.stop();
        }


        private void recursiveDeleteFolder( File folder ) {
            File[] allContents = folder.listFiles();
            if ( allContents != null ) {
                for ( File file : allContents ) {
                    recursiveDeleteFolder( file );
                }
            }
            folder.delete();
        }

    }

}
