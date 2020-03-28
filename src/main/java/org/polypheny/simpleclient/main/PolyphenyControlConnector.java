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


import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient.ChronosLogHandler;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONObject;


@Slf4j
class PolyphenyControlConnector {

    private final String controlUrl;
    private static int clientId = -1;
    private ChronosLogHandler chronosLogHandler;


    PolyphenyControlConnector( String controlUrl ) throws URISyntaxException {
        Unirest.setTimeouts( 0, 0 );
        Unirest.setConcurrency( 200, 100 );
        this.controlUrl = "http://" + controlUrl;
        WebSocket webSocket = new WebSocket( new URI( "ws://" + controlUrl + "/socket/" ) );
        webSocket.connect();
    }


    void stopPolypheny() {
        try {
            Unirest.post( controlUrl + "/control/stop" ).field( "clientId", clientId ).asString();
        } catch ( UnirestException e ) {
            log.error( "Error while stopping Polypheny-DB", e );
        }
    }


    void startPolypheny() {
        try {
            Unirest.post( controlUrl + "/control/start" ).field( "clientId", clientId ).asString();
        } catch ( UnirestException e ) {
            log.error( "Error while starting Polypheny-DB", e );
        }
    }


    void setConfig( String key, String value ) {
        JSONObject obj = new JSONObject();
        obj.put( "key", key );
        obj.put( "value", value );
        executePost( "/config/set", obj.toString() );
    }


    public String getConfig() {
        return executeGet( "/config/get" );
    }


    String getVersion() {
        return executeGet( "/control/version" );
    }


    void setChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        this.chronosLogHandler = chronosLogHandler;
    }


    private String executeGet( String command ) {
        HttpResponse<InputStream> httpResponse;
        try {
            httpResponse = Unirest.get( controlUrl + command ).asBinary();

            BufferedReader rd = new BufferedReader( new InputStreamReader( httpResponse.getRawBody() ) );
            StringBuilder response = new StringBuilder();
            String line;
            while ( (line = rd.readLine()) != null ) {
                response.append( line );
                response.append( '\r' );
            }
            rd.close();
            return response.toString();
        } catch ( IOException | UnirestException e ) {
            log.error( "Exception while sending request", e );
        }
        return null;
    }


    private void executePost( String command, String data ) {
        HttpResponse<InputStream> httpResponse;
        try {
            httpResponse = Unirest.post( controlUrl + command ).body( data ).asBinary();

            BufferedReader rd = new BufferedReader( new InputStreamReader( httpResponse.getRawBody() ) );
            StringBuilder response = new StringBuilder();
            String line;
            while ( (line = rd.readLine()) != null ) {
                response.append( line );
                response.append( '\r' );
            }
            rd.close();
        } catch ( IOException | UnirestException e ) {
            log.error( "Exception while sending request", e );
        }
    }


    private class WebSocket extends WebSocketClient {

        private Gson gson = new Gson();


        public WebSocket( URI serverUri ) {
            super( serverUri );
        }


        @Override
        public void onOpen( ServerHandshake handshakedata ) {
        }


        @Override
        public void onMessage( String message ) {
            Type type = new TypeToken<Map<String, String>>() {
            }.getType();
            Map<String, String> data = gson.fromJson( message, type );

            if ( data.containsKey( "clientId" ) ) {
                clientId = Integer.parseInt( data.get( "clientId" ) );
            }
            if ( data.containsKey( "logOutput" ) ) {
                if ( chronosLogHandler != null ) {
                    chronosLogHandler.publish( data.get( "logOutput" ) );
                    chronosLogHandler.flush();
                }
                log.info( data.get( "logOutput" ) );
            }
            if ( data.containsKey( "startOutput" ) ) {
                if ( chronosLogHandler != null ) {
                    chronosLogHandler.publish( data.get( "startOutput" ) );
                    chronosLogHandler.flush();
                }
                log.info( data.get( "logOutput" ) );
            }
            if ( data.containsKey( "stopOutput" ) ) {
                if ( chronosLogHandler != null ) {
                    chronosLogHandler.publish( data.get( "stopOutput" ) );
                    chronosLogHandler.flush();
                }
                log.info( data.get( "logOutput" ) );
            }
            if ( data.containsKey( "restartOutput" ) ) {
                if ( chronosLogHandler != null ) {
                    chronosLogHandler.publish( data.get( "restartOutput" ) );
                    chronosLogHandler.flush();
                }
                log.info( data.get( "logOutput" ) );
            }
            if ( data.containsKey( "updateOutput" ) ) {
                if ( chronosLogHandler != null ) {
                    chronosLogHandler.publish( data.get( "updateOutput" ) );
                    chronosLogHandler.flush();
                }
                log.info( data.get( "logOutput" ) );
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
