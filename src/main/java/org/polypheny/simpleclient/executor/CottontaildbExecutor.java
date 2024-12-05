/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 The Polypheny Project
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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kong.unirest.core.UnirestException;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.CottontailQuery;
import org.polypheny.simpleclient.query.CottontailQuery.QueryType;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.AbstractConfig;
import org.vitrivr.cottontail.CottontailKt;
import org.vitrivr.cottontail.config.Config;
import org.vitrivr.cottontail.config.ExecutionConfig;
import org.vitrivr.cottontail.config.MapDBConfig;
import org.vitrivr.cottontail.config.ServerConfig;
import org.vitrivr.cottontail.grpc.CottonDDLGrpc;
import org.vitrivr.cottontail.grpc.CottonDDLGrpc.CottonDDLBlockingStub;
import org.vitrivr.cottontail.grpc.CottonDDLGrpc.CottonDDLFutureStub;
import org.vitrivr.cottontail.grpc.CottonDMLGrpc;
import org.vitrivr.cottontail.grpc.CottonDMLGrpc.CottonDMLBlockingStub;
import org.vitrivr.cottontail.grpc.CottonDMLGrpc.CottonDMLStub;
import org.vitrivr.cottontail.grpc.CottonDQLGrpc;
import org.vitrivr.cottontail.grpc.CottonDQLGrpc.CottonDQLBlockingStub;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchedQueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Empty;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Schema;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Status;
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer;


@Slf4j
public class CottontaildbExecutor implements Executor {

    private final CsvWriter csvWriter;
    private final ManagedChannel channel;


    public CottontaildbExecutor( CsvWriter csvWriter, String hostname ) {
        super();
        this.csvWriter = csvWriter;

        channel = NettyChannelBuilder.forAddress( hostname, 1865 ).usePlaintext().maxInboundMetadataSize( 150_000_000 ).maxInboundMessageSize( 150_000_000 ).build();
        ensurePublicSchemaExists();
    }


    private void ensurePublicSchemaExists() {
        CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( channel );
        Iterator<Schema> schemas = stub.listSchemas( Empty.newBuilder().build() );
        while ( schemas.hasNext() ) {
            Schema existingSchema = schemas.next();
            if ( "public".equals( existingSchema.getName() ) ) {
                return;
            }
        }

        executeWrappedQuery( new CottontailQuery( QueryType.SCHEMA_CREATE, Schema.newBuilder().setName( "public" ).build() ), false );
    }


    private Object executeWrappedQuery( CottontailQuery cottontailQuery, boolean expectResult ) {
        switch ( cottontailQuery.type() ) {
            case QUERY: {
                final CottonDQLBlockingStub stub = CottonDQLGrpc.newBlockingStub( this.channel ).withDeadlineAfter( 300_000, TimeUnit.MILLISECONDS );
                try {
                    if ( expectResult ) {
                        final ArrayList<QueryResponseMessage> results = new ArrayList<>();
                        stub.query( QueryMessage.newBuilder().setQuery( (CottontailGrpc.Query) cottontailQuery.query() ).build() ).forEachRemaining( results::add );
                        return results;
                    } else {
                        stub.query( QueryMessage.newBuilder().setQuery( (CottontailGrpc.Query) cottontailQuery.query() ).build() );
                    }
                } catch ( StatusRuntimeException e ) {
                    log.error( "Unable to query cottontail. Query: {}", cottontailQuery.query(), e );
                    throw new RuntimeException( "Unable to query cottontail.", e );
                }
                break;
            }
            case QUERY_BATCH: {
                final CottonDQLBlockingStub stub = CottonDQLGrpc.newBlockingStub( this.channel ).withDeadlineAfter( 300_000, TimeUnit.MILLISECONDS );
                try {
                    if ( expectResult ) {
                        final ArrayList<QueryResponseMessage> results = new ArrayList<>();
                        stub.batchedQuery( BatchedQueryMessage.newBuilder().addAllQueries( (List<CottontailGrpc.Query>) cottontailQuery.query() ).build() ).forEachRemaining( results::add );
                        return results;
                    } else {
                        stub.batchedQuery( BatchedQueryMessage.newBuilder().addAllQueries( (List<CottontailGrpc.Query>) cottontailQuery.query() ).build() );
                    }
                } catch ( StatusRuntimeException e ) {
                    log.error( "Unable to batch query cottontail.", e );
                    throw new RuntimeException( "Unable to batch query cottontail.", e );
                }
                break;
            }
            case INSERT: {
                this.insert( ImmutableList.of( (CottontailGrpc.InsertMessage) cottontailQuery.query() ) );
                break;
            }
            case INSERT_BATCH: {
                this.insert( (List<InsertMessage>) cottontailQuery.query() );
                break;
            }
            case UPDATE: {
                CottonDMLBlockingStub managementStub = CottonDMLGrpc.newBlockingStub( this.channel );

                try {
                    if ( expectResult ) {
                        final ArrayList<QueryResponseMessage> results = new ArrayList<>();
                        managementStub.update( (CottontailGrpc.UpdateMessage) cottontailQuery.query() ).forEachRemaining( results::add );
                        return results;
                    } else {
                        managementStub.update( (CottontailGrpc.UpdateMessage) cottontailQuery.query() );
                    }
                } catch ( StatusRuntimeException e ) {
                    log.error( "Unable to execute cottontail update: {}", cottontailQuery.query(), e );
                    throw new RuntimeException( "Unable to execute cottontail update.", e );
                }
                break;
            }
            case DELETE: {
                CottonDMLBlockingStub managementStub = CottonDMLGrpc.newBlockingStub( this.channel );

                try {
                    if ( expectResult ) {
                        final ArrayList<QueryResponseMessage> results = new ArrayList<>();
                        managementStub.delete( (CottontailGrpc.DeleteMessage) cottontailQuery.query() ).forEachRemaining( results::add );
                        return results;
                    } else {
                        managementStub.delete( (CottontailGrpc.DeleteMessage) cottontailQuery.query() );
                    }
                } catch ( StatusRuntimeException e ) {
                    log.error( "Unable to execute cottontail delete: {}", cottontailQuery.query(), e );
                    throw new RuntimeException( "Unable to execute cottontail delete.", e );
                }
                break;
            }
            case SCHEMA_CREATE: {
                final CottonDDLFutureStub stub = CottonDDLGrpc.newFutureStub( channel );
                ListenableFuture<Status> future = stub.createSchema( (CottontailGrpc.Schema) cottontailQuery.query() );
                try {
                    future.get();
                } catch ( InterruptedException | ExecutionException e ) {
                    log.error( "Unable to create cottontail schema: {}.", cottontailQuery.query(), e );
                    throw new RuntimeException( "Unable to create cottontail schema.", e );
                }
                break;
            }
            case SCHEMA_DROP: {
                final CottonDDLFutureStub stub = CottonDDLGrpc.newFutureStub( channel );
                ListenableFuture<Status> future = stub.dropSchema( (CottontailGrpc.Schema) cottontailQuery.query() );
                try {
                    future.get();
                } catch ( InterruptedException | ExecutionException e ) {
                    log.error( "Unable to drop cottontail schema: {}.", cottontailQuery.query(), e );
                    throw new RuntimeException( "Unable to drop cottontail schema.", e );
                }
                break;
            }
            case ENTITY_CREATE: {
                final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( this.channel );
                try {
                    stub.createEntity( (CottontailGrpc.EntityDefinition) cottontailQuery.query() );
                } catch ( StatusRuntimeException e ) {
                    log.error( "Unable to create cottontail entity: {}", cottontailQuery.query(), e );
                    throw new RuntimeException( "Unable to create cottontail entity.", e );
                }
                break;
            }
            case ENTITY_DROP: {
                final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( this.channel );
                try {
                    stub.dropEntity( (CottontailGrpc.Entity) cottontailQuery.query() );
                } catch ( StatusRuntimeException e ) {
                    log.error( "Unable to drop cottontail entity: {}", cottontailQuery.query(), e );
                    throw new RuntimeException( "Unable to drop cottontail entity.", e );
                }
                break;
            }
            case TRUNCATE: {
                final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( this.channel );
                try {
                    stub.truncate( (CottontailGrpc.Entity) cottontailQuery.query() );
                } catch ( StatusRuntimeException e ) {
                    log.error( "Unable to truncate cottontail entity: {}", cottontailQuery.query(), e );
                    throw new RuntimeException( "Unable to truncate cottontail entity.", e );
                }
                break;
            }
        }

        return null;
    }


    private boolean insert( List<InsertMessage> messages ) {
        CottonDMLStub managementStub = CottonDMLGrpc.newStub( this.channel );
        final boolean[] status = { false, false }; /* {done, error}. */
        final StreamObserver<Status> observer = new StreamObserver<>() {

            @Override
            public void onNext( CottontailGrpc.Status value ) {
            }


            @Override
            public void onError( Throwable t ) {
                status[0] = true;
                status[1] = true;
            }


            @Override
            public void onCompleted() {
                status[0] = true;
            }
        };

        try {
            /* Start data transfer. */
            final StreamObserver<InsertMessage> sink = managementStub.insert( observer );
            for ( InsertMessage message : messages ) {
                sink.onNext( message );
            }
            sink.onCompleted(); /* Send commit message. */

            while ( !status[0] ) {
                Thread.yield();
            }
        } catch ( Exception e ) {
            log.error( "Exception while insert on cottontail db", e );
        }
        return !status[1];
    }


    private String wrapperToString( CottontailQuery query ) {
        String payload;
        switch ( query.type() ) {
            case QUERY_BATCH:
            case INSERT_BATCH:
                payload = String.join( ",", (List) query.query() );
            default:
                payload = query.query().toString();
        }

        payload = payload.replace( "\n", "" ).replace( "\r", "" );

        return query.type() + ":" + payload;
    }


    @Override
    public void reset() throws ExecutorException {
        throw new RuntimeException( "Unsupported operation" );
    }


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        long time;
        if ( query.getCottontail() != null ) {
            CottontailQuery cottontailQuery = query.getCottontail();
            try {
                long start = System.nanoTime();
                this.executeWrappedQuery( cottontailQuery, query.isExpectResultSet() );
                time = System.nanoTime() - start;
                if ( csvWriter != null ) {
                    csvWriter.appendToCsv( wrapperToString( cottontailQuery ), time );
                }
            } catch ( UnirestException e ) {
                throw new ExecutorException( e );
            }
        } else {
            throw new RuntimeException( "There is no Cottontail GRPC message defined for this query!" );
        }

        return time;
    }


    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        throw new RuntimeException( "Not supported by CottontailExecutor." );
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
        this.channel.shutdown();
    }


    @Override
    public void executeInsertList( List<BatchableInsert> batchList, AbstractConfig config ) throws ExecutorException {

        List<InsertMessage> insertMessages = new ArrayList<>( batchList.size() );
        for ( BatchableInsert insert : batchList ) {
            CottontailQuery insertMessage = insert.getCottontail();
            if ( insertMessage != null ) {
                if ( insertMessage.type() != QueryType.INSERT ) {
                    log.error( "Batchable Insert is not an InsertMessage. {}", insertMessage.query() );
                    throw new RuntimeException( "Batchable Insert is not an InsertMessage." );
                }

                insertMessages.add( (InsertMessage) insertMessage.query() );
            }
        }

        this.executeWrappedQuery( new CottontailQuery( QueryType.INSERT_BATCH, insertMessages ), false );
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

        protected String hostname;


        public CottontailExecutorFactory( String hostname ) {
            this.hostname = hostname;
        }


        @Override
        public CottontaildbExecutor createExecutorInstance( CsvWriter csvWriter ) {
            return new CottontaildbExecutor( csvWriter, hostname );
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

            recursiveDeleteFolder( folder );

            if ( !folder.exists() ) {
                if ( !folder.mkdirs() ) {
                    throw new RuntimeException( "Could not create data directory" );
                }
            }

            File dataFolder = new File( folder, "data" );
            Config config = new Config(
                    dataFolder.toPath(),
                    false,
                    new ServerConfig(),
                    new MapDBConfig(),
                    new ExecutionConfig()
            );
            this.embeddedServer = CottontailKt.embedded( config );
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
