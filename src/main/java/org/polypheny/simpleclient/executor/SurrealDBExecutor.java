/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-3/13/23, 5:13 PM The Polypheny Project
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

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.scenario.AbstractConfig;

@Slf4j
public class SurrealDBExecutor implements Executor {

    private final String host;
    private final CsvWriter csvWriter;
    private final HttpClient httpClient;
    private final WebSocketClient webSocketClient;
    private final Future<Session> clientSessionPromise;
    private final ClientEndPoint clientEndPoint;
    private final Session session;


    public SurrealDBExecutor( String host, CsvWriter csvWriter ) throws Exception {
        this.host = host;
        this.csvWriter = csvWriter;

        // Instantiate and configure HttpClient.
        this.httpClient = new HttpClient();

        // Instantiate WebSocketClient, passing HttpClient to the constructor.
        this.webSocketClient = new WebSocketClient( httpClient );
        // Configure WebSocketClient, for example:
        webSocketClient.setMaxTextMessageBufferSize( 8 * 1024 );

        // Start WebSocketClient; this implicitly starts also HttpClient.
        webSocketClient.start();

        // The client-side WebSocket EndPoint that
        // receives WebSocket messages from the server.
        this.clientEndPoint = new ClientEndPoint();
        // The server URI to connect to.
        URI serverURI = URI.create( "ws:" + host );

        // Connect the client EndPoint to the server.
        this.clientSessionPromise = webSocketClient.connect( clientEndPoint, serverURI );
        this.session = this.clientSessionPromise.get();
    }


    @Override
    public void reset() throws ExecutorException {
        try {
            webSocketClient.stop();
            webSocketClient.start();
        } catch ( Exception e ) {
            throw new ExecutorException( e );
        }

    }


    @Override
    public long executeQuery( Query query ) throws ExecutorException {
        long start = System.nanoTime();

        execute( query.getSurrealQl() );

        return System.nanoTime() - start;
    }


    private void execute( String query ) {
        clientEndPoint.onText( session, query );
    }


    @Override
    public long executeQueryAndGetNumber( Query query ) throws ExecutorException {
        throw new RuntimeException( "Not supported by " + getClass().getSimpleName() + "." );
    }


    @Override
    public void executeCommit() throws ExecutorException {
        // NoOp while websocket is used
    }


    @Override
    public void executeRollback() throws ExecutorException {
        // NoOp while websocket is used
    }


    @Override
    public void closeConnection() throws ExecutorException {
        try {
            this.webSocketClient.stop();
        } catch ( Exception e ) {
            throw new ExecutorException( e );
        }
    }


    @Override
    public void executeInsertList( List<BatchableInsert> batchList, AbstractConfig config ) throws ExecutorException {
        StringBuilder query = new StringBuilder( "INSERT INTO " + batchList.get( 0 ).getEntity() + " VALUES" );
        for ( BatchableInsert insert : batchList ) {
            query.append( String.format( "(%s)", String.join( ", ", insert.getRowValues() ) ) );
        }
        this.execute( query.toString() );
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


    public static class SurrealDBExecutorFactory extends ExecutorFactory {

        private final String host;


        public SurrealDBExecutorFactory( String host ) {
            this.host = host;
        }


        @Override
        public Executor createExecutorInstance( CsvWriter csvWriter ) {
            try {
                return new SurrealDBExecutor( host, csvWriter );
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }


    @WebSocket
    public static class ClientEndPoint {

        private Session session;


        @OnWebSocketConnect
        public void onConnect( Session session ) {
            // The WebSocket connection is established.

            // Store the session to be able to send data to the remote peer.
            this.session = session;

            // You may configure the session.
            session.setMaxTextMessageSize( 16 * 1024 );

            // You may immediately send a message to the remote peer.
            session.getRemote().sendString( "connected", WriteCallback.NOOP );
        }


        @OnWebSocketMessage
        public void onText( Session session, String text ) {
            // Obtain the RemoteEndpoint APIs.
            RemoteEndpoint remote = session.getRemote();

            try {
                // Send textual data to the remote peer.
                remote.sendString( "data" );

            } catch ( IOException x ) {
                // No need to rethrow or close the session.
                log.warn( "could not send data", x );
            }
        }

    }

}
