/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-8/8/24, 3:22 PM The Polypheny Project
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

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.polypheny.control.client.ClientData;
import org.polypheny.control.client.LogHandler;
import org.polypheny.control.client.PolyphenyControlConnector;

public class PolyphenyRemoteConnector implements PolyphenyConnector {

    PolyphenyControlConnector connector;


    public PolyphenyRemoteConnector( String controlUrl, ClientData clientData, LogHandler logHandler ) throws URISyntaxException {
        connector = new PolyphenyControlConnector( controlUrl, clientData, logHandler );
    }


    @Override
    public void stopPolypheny() {
        connector.stopPolypheny();
    }


    @Override
    public void startPolypheny() {
        connector.startPolypheny();
    }


    @Override
    public void purgePolyphenyFolder() {
        connector.purgePolyphenyFolder();
    }


    @Override
    public void updatePolypheny() {
        connector.updatePolypheny();
    }


    @Override
    public void setConfig( PolyphenyConnectorConfig config ) {
        Map<String, String> conf = new HashMap<>();
        conf.put( "pcrtl.pdbms.branch", config.branchDB() );
        conf.put( "pcrtl.ui.branch", config.branchUI() );
        conf.put( "pcrtl.java.heap", "10" );
        if ( config.buildUI() ) {
            conf.put( "pcrtl.buildmode", "both" );
        } else {
            conf.put( "pcrtl.buildmode", "pdb" );
        }
        conf.put( "pcrtl.clean.mode", "branchChange" );
        //conf.put( "pcrtl.plugins.purge", "onStartup" );
        conf.put( "pcrtl.plugins.purge", "never" );
        String args = "";
        if ( config.resetCatalog() ) {
            args += "-resetCatalog ";
        }
        if ( config.memoryCatalog() ) {
            args += "-memoryCatalog ";
        }
        conf.put( "pcrtl.pdbms.args", args.trim() );
        connector.setConfig( conf );
    }


    @Override
    public String getConfig() {
        return connector.getConfig();
    }


    @Override
    public String getVersion() {
        return connector.getVersion();
    }


    @Override
    public int checkForAnyRunningPolyphenyInstances() {
        return connector.checkForAnyRunningPolyphenyInstances();
    }

}
