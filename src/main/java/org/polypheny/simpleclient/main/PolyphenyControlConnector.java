/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
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
 *
 */

package org.polypheny.simpleclient.main;


import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import kong.unirest.UnirestException;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Slf4j
class PolyphenyControlConnector {

    private final String controlUrl;
    private static int clientId = -1;

    private final Gson gson = new Gson();


    PolyphenyControlConnector( String controlUrl ) throws URISyntaxException {
        Unirest.config().connectTimeout( 0 );
        Unirest.config().socketTimeout( 0 );
        Unirest.config().concurrency( 200, 100 );
        this.controlUrl = "http://" + controlUrl;
        WebSocket webSocket = new WebSocket( new URI( "ws://" + controlUrl + "/socket/" ) );
        webSocket.connect();
    }


    void stopPolypheny() {
        setClientType(); // Set the client type (again) - does not hurt and makes sure its set
        try {
            Unirest.post( controlUrl + "/control/stop" ).field( "clientId", clientId ).asString();
        } catch ( UnirestException e ) {
            log.error( "Error while stopping Polypheny-DB", e );
        }
    }


    void startPolypheny() {
        setClientType(); // Set the client type (again) - does not hurt and makes sure its set
        try {
            Unirest.post( controlUrl + "/control/start" ).field( "clientId", clientId ).asString();
        } catch ( UnirestException e ) {
            log.error( "Error while starting Polypheny-DB", e );
        }
    }


    public void updatePolypheny() {
        // Check if in status idling
        String status = getStatus();
        if ( !status.equals( "idling" ) ) {
            throw new RuntimeException( "Unable to update Polypheny while it is running" );
        }
        // Trigger update
        try {
            Unirest.post( controlUrl + "/control/update" ).field( "clientId", clientId ).asString();
        } catch ( UnirestException e ) {
            log.error( "Error while updating Polypheny-DB", e );
        }
        // Wait for update to finish
        status = getStatus();
        do {
            try {
                TimeUnit.SECONDS.sleep( 1 );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( "Unexpected interrupt", e );
            }
        } while ( !status.equals( "idling" ) );
    }


    void setConfig( Map<String, String> map ) {
        JSONObject obj = new JSONObject();
        for ( Map.Entry<String, String> entry : map.entrySet() ) {
            obj.put( entry.getKey(), entry.getValue() );
        }
        try {
            Unirest.post( controlUrl + "/config/set" ).field( "clientId", clientId ).field( "config", obj.toString() ).asString();
        } catch ( UnirestException e ) {
            log.error( "Error while setting client type", e );
        }
    }


    void setClientType() {
        try {
            Unirest.post( controlUrl + "/client/type" ).field( "clientId", clientId ).field( "clientType", "BENCHMARKER" ).asString();
        } catch ( UnirestException e ) {
            log.error( "Error while setting client type", e );
        }
    }


    public String getConfig() {
        return executeGet( "/config/get" );
    }


    String getVersion() {
        return executeGet( "/control/version" );
    }


    String getStatus() {
        return gson.fromJson( executeGet( "/control/status" ), String.class );
    }


    private String executeGet( String command ) {
        HttpResponse<String> httpResponse;
        try {
            httpResponse = Unirest.get( controlUrl + command ).asString();
            return httpResponse.getBody();
        } catch ( UnirestException e ) {
            log.error( "Exception while sending request", e );
        }
        return null;
    }


    private void executePost( String command, String data ) {
        try {
            Unirest.post( controlUrl + command ).body( data ).asString();
        } catch ( UnirestException e ) {
            log.error( "Exception while sending request", e );
        }
    }


    private class WebSocket extends WebSocketClient {

        private final Gson gson = new Gson();
        private final Logger CONTROL_MESSAGES_LOGGER = LoggerFactory.getLogger( "CONTROL_MESSAGES_LOGGER" );


        public WebSocket( URI serverUri ) {
            super( serverUri );
        }


        @Override
        public void onOpen( ServerHandshake handshakedata ) {
        }


        @Override
        public void onMessage( String message ) {
            if ( message.startsWith( "{\"version\":{" ) ) {
                return;
            }
            Type type = new TypeToken<Map<String, String>>() {
            }.getType();
            Map<String, String> data = gson.fromJson( message, type );

            if ( data.containsKey( "clientId" ) ) {
                clientId = Integer.parseInt( data.get( "clientId" ) );
                setClientType();
            }
            if ( data.containsKey( "logOutput" ) ) {
                CONTROL_MESSAGES_LOGGER.info( data.get( "logOutput" ) );
            }
            if ( data.containsKey( "startOutput" ) ) {
                CONTROL_MESSAGES_LOGGER.info( data.get( "startOutput" ) );
            }
            if ( data.containsKey( "stopOutput" ) ) {
                CONTROL_MESSAGES_LOGGER.info( data.get( "stopOutput" ) );
            }
            if ( data.containsKey( "restartOutput" ) ) {
                CONTROL_MESSAGES_LOGGER.info( data.get( "restartOutput" ) );
            }
            if ( data.containsKey( "updateOutput" ) ) {
                String logStr = data.get( "updateOutput" );
                if ( logStr.startsWith( "Task :" ) && (logStr.endsWith( "started" ) || logStr.endsWith( "skipped" ) || logStr.endsWith( "UP-TO-DATE" ) || logStr.endsWith( "SUCCESS" )) ) {
                    // Ignore this to avoid cluttering the log. These are gradle log massage where everything is fine
                } else {
                    CONTROL_MESSAGES_LOGGER.info( logStr );
                }
            }
        }


        @Override
        public void onClose( int code, String reason, boolean remote ) {
        }


        @Override
        public void onError( Exception ex ) {

        }
    }

}
