package org.polypheny.simpleclient.scenario.oltpbench.tpch;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import lombok.SneakyThrows;
import org.polypheny.simpleclient.scenario.oltpbench.AbstractOltpBenchConfig;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class TpchConfig extends AbstractOltpBenchConfig {

    public String rate;
    public int batchSize;

    public List<String> transactionTypes;
    public int iterations;


    public TpchConfig( Properties properties, int multiplier ) {
        super( properties, multiplier, "tpch", "oltpbench-polypheny" );

        rate = getStringProperty( properties, "rate" );
        batchSize = 128;

        transactionTypes = Arrays.asList( getStringProperty( properties, "transactionTypes" ).split( "," ) );
        iterations = multiplier;
    }


    public TpchConfig( Map<String, String> cdl ) {
        super( cdl, "tpch", cdl.get( "store" ) );

        rate = cdl.get( "rate" );
        batchSize = 128;

        transactionTypes = Arrays.asList( cdl.get( "transactionTypes" ).split( "," ) );
        iterations = Integer.parseInt( cdl.get( "iterations" ) );
    }


    @SneakyThrows
    @Override
    public String toXml( String dbType, String driver, String dbUrl, String username, String password, String ddlFileName, String dialectFileName, String isolation ) {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

        // root elements
        Document doc = docBuilder.newDocument();
        doc.setXmlStandalone( true );
        Element rootElement = doc.createElement( "parameters" );
        doc.appendChild( rootElement );

        // Executor specific elements
        createAndAppendElement( doc, rootElement, "dbtype", dbType );
        createAndAppendElement( doc, rootElement, "driver", driver );
        createAndAppendElement( doc, rootElement, "DBUrl", dbUrl );
        createAndAppendElement( doc, rootElement, "username", username );
        createAndAppendElement( doc, rootElement, "password", password );
        createAndAppendElement( doc, rootElement, "ddlFileName", ddlFileName );
        //createAndAppendElement( doc, rootElement, "dialectFileName", ddlFileName );
        createAndAppendElement( doc, rootElement, "isolation", isolation );
        createAndAppendElement( doc, rootElement, "batchsize", batchSize + "" );

        // General OLTPbench elements
        createAndAppendElement( doc, rootElement, "scalefactor", scaleFactor + "" );
        createAndAppendElement( doc, rootElement, "terminals", numberOfThreads + "" );

        createAndAppendElement( doc, rootElement, "datadir", "data/tpch" );
        createAndAppendElement( doc, rootElement, "fileFormat", "tbl" );

        // -- Scenario specific elements --

        // Works
        Element worksElement = doc.createElement( "works" );
        rootElement.appendChild( worksElement );
        for ( int i = 0; i < iterations; i++ ) {
            Element workElement = doc.createElement( "work" );
            worksElement.appendChild( workElement );
            createAndAppendElement( doc, workElement, "rate", rate );
            createAndAppendElement( doc, workElement, "serial", "true" );
            createAndAppendElement( doc, workElement, "weights", "all" );
        }

        // Transaction types
        Element transactionTypesElement = doc.createElement( "transactiontypes" );
        rootElement.appendChild( transactionTypesElement );
        for ( String trxTyp : transactionTypes ) {
            createAndAppendTransactionTypeElement( doc, transactionTypesElement, trxTyp );
        }

        StringWriter sw = new StringWriter();
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "no" );
        transformer.setOutputProperty( OutputKeys.METHOD, "xml" );
        transformer.setOutputProperty( OutputKeys.INDENT, "yes" );
        transformer.setOutputProperty( OutputKeys.ENCODING, "UTF-8" );

        transformer.transform( new DOMSource( doc ), new StreamResult( sw ) );
        return sw.toString();
    }


}
