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


import ch.unibas.dmi.dbis.chronos.agent.AbstractChronosAgent;
import ch.unibas.dmi.dbis.chronos.agent.ChronosHttpClient.ChronosLogHandler;
import ch.unibas.dmi.dbis.chronos.agent.ChronosJob;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.cli.ChronosCommand;
import org.polypheny.simpleclient.cli.Main;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;
import org.polypheny.simpleclient.executor.PostgresExecutor;
import org.polypheny.simpleclient.scenario.gavel.Config;
import org.polypheny.simpleclient.scenario.gavel.Gavel;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


@Slf4j
public class ChronosAgent extends AbstractChronosAgent {


    public final String[] supports;
    PolyphenyControlConnector polyphenyControlConnector = null;


    public ChronosAgent( InetAddress address, int port, boolean secure, boolean useHostname, String environment, String supports ) {
        super( address, port, secure, useHostname, environment );
        this.supports = new String[]{ supports };
        try {
            polyphenyControlConnector = new PolyphenyControlConnector( ChronosCommand.polyphenyDbHost + ":8070" );
        } catch ( URISyntaxException e ) {
            log.error( "Exception while connecting to Polypheny Control", e );
        }
    }


    @Override
    protected String[] getSupportedSystemNames() {
        return supports;
    }


    @Override
    protected Object prepare( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        // Parse CDL
        Map<String, String> settings = null;
        try {
            settings = readCDL( new ByteArrayInputStream( chronosJob.cdl.getBytes( StandardCharsets.UTF_8 ) ) );
        } catch ( XPathExpressionException | ParserConfigurationException | SAXException | IOException e ) {
            log.error( "Exception while parsing cdl", e );
        }
        Config config = new Config( settings );
        Gavel gavel = new Gavel( ChronosCommand.polyphenyDbHost, config );

        // Stop Polypheny
        polyphenyControlConnector.stopPolypheny();
        try {
            TimeUnit.SECONDS.sleep( 3 );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( "Unexpected interrupt", e );
        }

        // Update settings
        Map<String, String> conf = new HashMap<>();
        conf.put( "pcrtl.pdbms.branch", config.pdbBranch.trim() );
        conf.put( "pcrtl.ui.branch", config.puiBranch.trim() );
        conf.put( "pcrtl.java.heap", "10" );
        conf.put( "pcrtl.buildmode", "both" );
        conf.put( "pcrtl.clean", "keep" );
        polyphenyControlConnector.setConfig( conf );

        // Pull branch and update polypheny
        polyphenyControlConnector.updatePolypheny();

        // Start Polypheny
        polyphenyControlConnector.startPolypheny();
        try {
            TimeUnit.SECONDS.sleep( 5 );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( "Unexpected interrupt", e );
        }

        // Create schema
        try {
            gavel.createSchema();
        } catch ( SQLException e ) {
            log.error( "Error while creating schema", e );
        }

        // Insert data
        ProgressReporter progressReporter = new ChronosProgressReporter(
                chronosJob,
                this,
                config.numberOfUserGenerationThreads + config.numberOfAuctionGenerationThreads, config.progressReportBase );
        try {
            gavel.buildDatabase( progressReporter );
        } catch ( SQLException e ) {
            log.error( "Error while inserting data", e );
        }

        return null;
    }


    @Override
    protected Object warmUp( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        return Boolean.TRUE;
    }


    @Override
    protected Object execute( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {

        boolean warmUp = false;
        if ( o != null ) {
            warmUp = (Boolean) o;
        }

        Map<String, String> settings = null;
        try {
            settings = readCDL( new ByteArrayInputStream( chronosJob.cdl.getBytes( StandardCharsets.UTF_8 ) ) );
        } catch ( XPathExpressionException | ParserConfigurationException | SAXException | IOException e ) {
            log.error( "Exception while parsing cdl", e );
        }

        assert settings != null;
        Config config = new Config( settings );
        Gavel gavel = new Gavel( ChronosCommand.polyphenyDbHost, config );

        final CsvWriter csvWriter;
        if ( Main.WRITE_CSV ) {
            csvWriter = new CsvWriter( outputDirectory.getPath() + File.separator + "results.csv" );
        } else {
            csvWriter = null;
        }
        ProgressReporter progressReporter = new ChronosProgressReporter( chronosJob, this, config.numberOfThreads, config.progressReportBase );
        long runtime = 0;
        if ( config.store.equals( "polypheny" ) ) {
            /*
            LOG.log( Level.INFO, "Setting Icarus configuration" );
            settings.forEach( ( key, value ) -> {
                if ( key.startsWith( "icarus_" ) ) {
                    icarusWrapperConnector.setConfig( key.substring( 7 ), value );
                }
            } );
            */

            try {
                runtime = gavel.execute( progressReporter, csvWriter, outputDirectory, new PolyphenyDbExecutor( ChronosCommand.polyphenyDbHost, config ), warmUp );
            } catch ( SQLException e ) {
                log.error( "Error while executing query", e );
            }
            gavel.analyze( properties );
            gavel.analyzeMeasuredTime( properties );/*
            // store icarus properties for documentation
            try {
                Properties p = new Properties();
                p.load( new StringReader( icarusWrapperConnector.getConfig() ) );
                writeConfig( p,outputDirectory.getPath() + File.separator + "icarus.properties" );
            } catch ( IOException e ) {
                e.printStackTrace();
            }*/
            // Store Polypheny version for documentation
            try {
                FileWriter fw = new FileWriter( outputDirectory.getPath() + File.separator + "polypheny.version" );
                fw.append( polyphenyControlConnector.getVersion() );
                fw.close();
            } catch ( IOException e ) {
                log.error( "Error while logging polypheny version", e );
            }
        } else {
            HashMap<String, Properties> storeProperties;
            try {
                storeProperties = getStoreProperties( polyphenyControlConnector );
                if ( config.store.equals( "postgres" ) ) {
                    storeProperties.get( "postgressqlhdd" ).put( "host", storeProperties.get( "postgressqlhdd" ).getProperty( "host" ).replaceAll( "127.0.0.1", ChronosCommand.polyphenyDbHost ) );
                    runtime = gavel.execute( progressReporter, csvWriter, outputDirectory, new PostgresExecutor( storeProperties.get( "postgressqlhdd" ) ), warmUp );
                } else {
                    System.err.println( "Unknown Store: " + config.store );
                }
            } catch ( IOException | SQLException e ) {
                log.error( "Error while executing query", e );
            }
            gavel.analyzeMeasuredTime( properties );
        }
        properties.put( "runtime", runtime );
        log.info( gavel.getTimesAsString( properties ) );

        return null;
    }


    @Override
    protected Object analyze( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        return null;
    }


    @Override
    protected Object clean( ChronosJob chronosJob, final File inputDirectory, final File outputDirectory, Properties properties, Object o ) {
        return null;
    }


    private Map<String, String> readCDL( InputStream is ) throws XPathExpressionException, ParserConfigurationException, IOException, SAXException {
        Map<String, String> settings = new HashMap<>();
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse( is );

        doc.getDocumentElement().normalize();

        XPathFactory xpathFactory = XPathFactory.newInstance();
        // XPath to find empty text nodes.
        XPathExpression xpathExp = xpathFactory.newXPath().compile( "//text()[normalize-space(.) = '']" );
        NodeList emptyTextNodes = (NodeList) xpathExp.evaluate( doc, XPathConstants.NODESET );

        // Remove each empty text node from document.
        for ( int i = 0; i < emptyTextNodes.getLength(); i++ ) {
            Node emptyTextNode = emptyTextNodes.item( i );
            emptyTextNode.getParentNode().removeChild( emptyTextNode );
        }
        if ( doc.getDocumentElement().getNodeName().equals( "chronos" ) ) {
            if ( doc.getDocumentElement().getChildNodes().item( 1 ).getNodeName().equals( "evaluation" ) ) {
                NodeList nList = doc.getDocumentElement().getChildNodes().item( 1 ).getChildNodes();
                for ( int i = 0; i < nList.getLength(); i++ ) {
                    Node nNode = nList.item( i );
                    if ( nNode.hasChildNodes() ) {
                        settings.put( nNode.getNodeName(), nNode.getFirstChild().getNodeValue() );
                    }
                }
            } else {
                log.warn( "Not a evaluation job!" );
            }
        } else {
            log.warn( "Not a valid CDL!" );
        }
        return settings;
    }


    @Override
    protected void aborted( ChronosJob arg0 ) {

    }


    @Override
    protected void failed( ChronosJob chronosJob ) {

    }


    void updateProgress( ChronosJob job, int progress ) {
        setProgress( job, (byte) progress );
    }


    @Override
    protected void addChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        polyphenyControlConnector.setChronosLogHandler( chronosLogHandler );
    }


    @Override
    protected void removeChronosLogHandler( ChronosLogHandler chronosLogHandler ) {
        polyphenyControlConnector.setChronosLogHandler( null );
    }


    private HashMap<String, Properties> getStoreProperties( PolyphenyControlConnector polyphenyControlConnector ) throws IOException {
        Properties properties = new Properties();
        properties.load( new StringReader( polyphenyControlConnector.getConfig() ) );
        String PREFIX = "executor";
        HashMap<String, Properties> props = new HashMap<>();
        String key, value, temp, name;
        // Iterate through the properties file
        for ( Map.Entry<Object, Object> entry : properties.entrySet() ) {
            key = (String) entry.getKey();
            value = (String) entry.getValue();

            // Check if it is a executor property
            if ( key.startsWith( PREFIX + "." ) && !key.startsWith( PREFIX + ".general." ) ) {
                // remove the 'executor.' part
                temp = key.substring( PREFIX.length() + 1 );

                // extract name and the key part
                if ( !temp.contains( "." ) ) {
                    name = temp;
                    key = "__CLASS__";
                } else {
                    name = temp.substring( 0, temp.indexOf( "." ) );
                    key = temp.substring( name.length() + 1 );
                }

                if ( !props.containsKey( name ) ) {
                    props.put( name, new Properties() );
                }

                props.get( name ).put( key, value );
            }
        }
        return props;
    }
}