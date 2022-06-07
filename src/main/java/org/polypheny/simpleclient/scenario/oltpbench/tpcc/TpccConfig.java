package org.polypheny.simpleclient.scenario.oltpbench.tpcc;

import java.io.StringWriter;
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

public class TpccConfig extends AbstractOltpBenchConfig {


    public long time;
    public String rate;
    public int batchSize;

    public int newOrderWeight;
    public int paymentWeight;
    public int orderStatusWeight;
    public int deliveryWeight;
    public int stockLevelWeight;


    public TpccConfig( Properties properties, int multiplier ) {
        super( properties, multiplier, "tpcc", "oltpbench-polypheny" );

        time = getLongProperty( properties, "time" );
        rate = getStringProperty( properties, "rate" );
        batchSize = 128;

        newOrderWeight = getIntProperty( properties, "newOrderWeight" );
        paymentWeight = getIntProperty( properties, "paymentWeight" );
        orderStatusWeight = getIntProperty( properties, "orderStatusWeight" );
        deliveryWeight = getIntProperty( properties, "deliveryWeight" );
        stockLevelWeight = getIntProperty( properties, "stockLevelWeight" );
    }


    public TpccConfig( Map<String, String> cdl ) {
        super( cdl, "tpcc", cdl.get( "store" ) );

        time = Long.parseLong( cdl.get( "time" ) );
        rate = cdl.get( "rate" );
        batchSize = 128;

        newOrderWeight = Integer.parseInt( cdl.get( "newOrderWeight" ) );
        paymentWeight = Integer.parseInt( cdl.get( "paymentWeight" ) );
        orderStatusWeight = Integer.parseInt( cdl.get( "orderStatusWeight" ) );
        deliveryWeight = Integer.parseInt( cdl.get( "deliveryWeight" ) );
        stockLevelWeight = Integer.parseInt( cdl.get( "stockLevelWeight" ) );
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

        // -- Scenario specific elements --

        // Works
        Element worksElement = doc.createElement( "works" );
        rootElement.appendChild( worksElement );
        Element workElement = doc.createElement( "work" );
        worksElement.appendChild( workElement );
        createAndAppendElement( doc, workElement, "time", time + "" );
        createAndAppendElement( doc, workElement, "rate", rate );
        createAndAppendElement( doc, workElement, "warmup", warmupTime + "" );
        createAndAppendElement( doc, workElement, "weights",
                newOrderWeight + "," +
                        paymentWeight + "," +
                        orderStatusWeight + "," +
                        deliveryWeight + "," +
                        stockLevelWeight
        );

        // Transaction types
        Element transactionTypesElement = doc.createElement( "transactiontypes" );
        rootElement.appendChild( transactionTypesElement );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "NewOrder" );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "Payment" );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "OrderStatus" );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "Delivery" );
        createAndAppendTransactionTypeElement( doc, transactionTypesElement, "StockLevel" );

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
