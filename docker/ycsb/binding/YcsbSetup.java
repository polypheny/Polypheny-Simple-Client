package org.polypheny.simpleclient.scenario.ycsb.binding;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import org.polypheny.jdbc.PolyConnection;

public final class YcsbSetup {
    public static void main( String[] args ) throws Exception {
        if ( args.length != 2 ) {
            throw new IllegalArgumentException( "Expected <config.properties or -> <target>" );
        }
        Properties properties = new Properties();
        try ( var input = args[0].equals( "-" ) ? System.in : Files.newInputStream( Path.of( args[0] ) ) ) {
            properties.load( input );
        }
        switch ( args[1] ) {
            case "polypheny-ready" -> checkPolypheny( properties );
            case "polypheny" -> setupPolypheny( properties );
            case "mongodb" -> setupMongoDb( properties );
            case "postgrenosql", "postgresql" -> setupPostgres( properties, args[1] );
            default -> throw new IllegalArgumentException( "Unknown target: " + args[1] );
        }
        System.out.println( "SETUP PASS: " + args[1] );
    }

    private static void checkPolypheny( Properties properties ) throws Exception {
        Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );
        try ( var connection = DriverManager.getConnection( required( properties, "target.polypheny.db.url" ),
                properties.getProperty( "target.polypheny.db.user", "pa" ), properties.getProperty( "target.polypheny.db.passwd", "" ) );
                var schemas = connection.getMetaData().getSchemas() ) {
            schemas.next();
        }
    }

    private static void setupMongoDb( Properties properties ) throws Exception {
        String table = identifier( properties.getProperty( "ycsb.table", "usertable" ) );
        MongoClientURI uri = new MongoClientURI( required( properties, "target.mongodb.mongodb.url" ) );
        if ( uri.getDatabase() == null ) {
            throw new IllegalArgumentException( "MongoDB URL needs a database name" );
        }
        try ( MongoClient client = new MongoClient( uri ) ) {
            client.getDatabase( uri.getDatabase() ).createCollection( table );
        }
    }

    private static void setupPostgres( Properties properties, String target ) throws Exception {
        boolean relational = target.equals( "postgresql" );
        String prefix = "target." + target + (relational ? ".db." : ".postgrenosql.");
        String table = identifier( properties.getProperty( "target." + target + ".table",
                properties.getProperty( "ycsb.table", "usertable" ) ) );
        StringBuilder ddl = new StringBuilder( "CREATE TABLE " ).append( table )
                .append( " (YCSB_KEY VARCHAR(255) PRIMARY KEY" );
        if ( relational ) {
            String fieldPrefix = identifier( properties.getProperty( "ycsb.fieldnameprefix", "field" ) );
            int fieldCount = Integer.parseInt( required( properties, "ycsb.fieldcount" ) );
            for ( int i = 0; i < fieldCount; i++ ) {
                ddl.append( ", " ).append( fieldPrefix ).append( i ).append( " TEXT" );
            }
        } else {
            ddl.append( ", YCSB_VALUE JSONB NOT NULL" );
        }
        Class.forName( "org.postgresql.Driver" );
        try ( var connection = DriverManager.getConnection( required( properties, prefix + "url" ),
                required( properties, prefix + "user" ),
                properties.getProperty( prefix + "passwd", "" ) );
                var statement = connection.createStatement() ) {
            statement.executeUpdate( ddl.append( ')' ).toString() );
        }
    }

    private static void setupPolypheny( Properties properties ) throws Exception {
        String namespace = identifier( required( properties, "target.polypheny.db.namespace" ) );
        String table = identifier( properties.getProperty( "ycsb.table", "usertable" ) );
        String store = identifier( required( properties, "setup.store" ) );
        Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );
        try ( var connection = DriverManager.getConnection( required( properties, "target.polypheny.db.url" ),
                properties.getProperty( "target.polypheny.db.user", "pa" ), properties.getProperty( "target.polypheny.db.passwd", "" ) ) ) {
            connection.setAutoCommit( true );
            try ( var namespaces = connection.getMetaData().getSchemas() ) {
                while ( namespaces.next() ) {
                    if ( namespace.equals( namespaces.getString( "TABLE_SCHEM" ) ) ) {
                        throw new IllegalArgumentException( "Namespace already exists: " + namespace
                                + ". Use a fresh namespace for setup, or run data/workload separately." );
                    }
                }
            }
            var statement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            if ( Boolean.parseBoolean( properties.getProperty( "setup.createStore", "true" ) ) ) {
                String settings = new Gson().toJson( Map.of( "mode", "remote",
                        "host", required( properties, "setup.mongoHost" ), "port", properties.getProperty( "setup.mongoPort", "27017" ),
                        "trxLifetimeLimit", "1209600" ) );
                statement.execute( "public", "sql", "ALTER ADAPTERS ADD \"" + store
                        + "\" USING 'MongoDB' AS STORE WITH '" + settings.replace( "'", "''" ) + "'" );
                System.out.println( "Created MongoDB store: " + store );
            }
            statement.execute( "public", "sql", "CREATE DOCUMENT NAMESPACE \"" + namespace + "\"" );
            statement.execute( namespace, "mongo", "db.createCollection(\"" + table + "\").store(\"" + store + "\")" );
            System.out.println( "Created " + namespace + "." + table + " on " + store );
        }
    }

    private static String identifier( String value ) {
        if ( !value.matches( "[a-z][a-z0-9_]*" ) ) {
            throw new IllegalArgumentException( "Invalid identifier: " + value );
        }
        return value;
    }

    private static String required( Properties properties, String key ) {
        String value = properties.getProperty( key );
        if ( value == null || value.isBlank() ) {
            throw new IllegalArgumentException( "Missing property: " + key );
        }
        return value.trim();
    }
}
