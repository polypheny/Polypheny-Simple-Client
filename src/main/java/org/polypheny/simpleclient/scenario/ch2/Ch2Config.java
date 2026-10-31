package org.polypheny.simpleclient.scenario.ch2;

import java.util.Properties;


public class Ch2Config {

    public final String imageName;
    public final String network;
    public final int warehouses;
    public final int transactionClients;
    public final int analyticalClients;
    public final int durationSeconds;


    public Ch2Config( Properties properties ) {
        imageName = properties.getProperty( "imageName", "polypheny-ch2:test" ).trim();
        network = properties.getProperty( "network", "benchmark-net" ).trim();

        warehouses = Integer.parseInt( properties.getProperty( "warehouses", "1" ).trim() );
        transactionClients = Integer.parseInt( properties.getProperty( "transactionClients", "1" ).trim() );
        analyticalClients = Integer.parseInt( properties.getProperty( "analyticalClients", "0" ).trim() );
        durationSeconds = Integer.parseInt( properties.getProperty( "durationSeconds", "10" ).trim() );

        if ( imageName.isBlank() || network.isBlank() ) {
            throw new IllegalArgumentException( "Image name and Docker network must not be blank" );
        }

        if ( warehouses < 1 || transactionClients < 1 || analyticalClients < 0 || durationSeconds < 1 ) {
            throw new IllegalArgumentException( "Warehouses, transaction clients and duration must be > 0; analytical clients must be >= 0" );}
    }
}
