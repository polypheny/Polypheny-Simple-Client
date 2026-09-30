package org.polypheny.simpleclient.scenario.ycsb.binding;

import com.google.gson.JsonObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.multimodel.DocumentResult;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.Result;
import org.polypheny.jdbc.multimodel.ScalarResult;
import org.polypheny.jdbc.types.PolyDocument;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;


public class PolyphenyMongoBinding extends DB {

    private Connection connection;
    private PolyStatement statement;
    private String namespace;
    private boolean errorReported;


    @Override
    public void init() throws DBException {
        Properties properties = getProperties();
        namespace = properties.getProperty( "db.namespace", "ycsb" );

        try {
            Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );

            connection = DriverManager.getConnection(
                    properties.getProperty( "db.url" ),
                    properties.getProperty( "db.user", "pa" ),
                    properties.getProperty( "db.passwd", "" )
            );

            connection.setAutoCommit( true );

            statement = connection.unwrap( PolyConnection.class )
                    .createPolyStatement();

        } catch ( Exception e ) {
            if ( connection != null ) {
                try {
                    connection.close();
                } catch ( SQLException closeError ) {
                    e.addSuppressed( closeError );
                }
            }

            throw new DBException( e );
        }
    }


    @Override
    public Status insert( String table, String key, Map<String, ByteIterator> values ) {
        try {
            JsonObject document = new JsonObject();
            document.addProperty( "_id", key );

            for ( var entry : values.entrySet() ) {
                document.addProperty( entry.getKey(), entry.getValue().toString() );
            }

            String query = collection( table ) + ".insert(" + toMongoJson( document ) + ")";

            Result response = statement.execute( namespace, "mongo", query );
            long inserted = getUpdateCount( response );

            return inserted == 1 ? Status.OK : Status.ERROR;

        } catch ( Exception e ) {
            return reportError( e );
        }
    }


    @Override
    public Status read( String table, String key, Set<String> fields, Map<String, ByteIterator> result ) {
        try {
            JsonObject filter = new JsonObject();
            filter.addProperty( "_id", key );

            String query = collection( table ) + ".find(" + filter + ")";

            DocumentResult documents = statement.execute( namespace, "mongo", query ).unwrap( DocumentResult.class );

            boolean found = false;

            for ( PolyDocument document : documents ) {
                if ( found ) {
                    throw new IllegalStateException( "Multiple documents found for key: " + key );
                }

                fillMap( result, document, fields );
                found = true;
            }

            return found ? Status.OK : Status.NOT_FOUND;

        } catch ( Exception e ) {
            return reportError( e );
        }
    }

    private static void fillMap( Map<String, ByteIterator> result, PolyDocument document, Set<String> fields ) throws SQLException {
        for ( var entry : document.entrySet() ) {
            String field = entry.getKey();

            if ( "_id".equals( field ) ) {
                continue;
            }

            if ( fields != null && !fields.contains( field ) ) {
                continue;
            }

            result.put( field, new StringByteIterator( entry.getValue().asString() ) );
        }

        if ( fields != null && !result.keySet().containsAll( fields ) ) {
            throw new IllegalStateException( "Document does not contain all requested fields" );
        }
    }


    private static long getUpdateCount( Result response ) throws SQLException {
        if ( response instanceof ScalarResult scalar ) {
            return scalar.getScalar();
        }

        if ( response instanceof DocumentResult documents ) {
            long count = 0;

            for ( PolyDocument document : documents ) {
                var updateCount = document.get( "updateCount" );

                if ( updateCount == null ) {
                    throw new IllegalStateException( "Insert result has no updateCount: " + document );
                }

                count += updateCount.asLong();
            }

            return count;
        }

        throw new IllegalStateException( "Unexpected insert result: " + response.getClass().getName() );
    }

    private static String toMongoJson( JsonObject document ) {
        StringBuilder json = new StringBuilder( "{" );
        String separator = "";

        for ( var entry : document.entrySet() ) {
            json.append( separator ).append( mongoString( entry.getKey() ) ).append( ":" ).append( mongoString( entry.getValue().getAsString() ) );

            separator = ",";
        }

        return json.append( "}" ).toString();
    }

    private static String mongoString( String value ) {
        StringBuilder json = new StringBuilder( "\"" );

        for ( int i = 0; i < value.length(); i++ ) {
            String hex = Integer.toHexString( value.charAt( i ) );

            json.append( "\\u" ).append( "0".repeat( 4 - hex.length() ) ).append( hex );
        }

        return json.append( "\"" ).toString();
    }


    private static String collection( String table ) {
        if ( table == null || !table.matches( "[A-Za-z_][A-Za-z0-9_]*" ) ) {
            throw new IllegalArgumentException( "Unsupported collection name: " + table );
        }

        return "db." + table;
    }


    private Status reportError( Exception e ) {
        if ( !errorReported ) {
            e.printStackTrace( System.err );
            errorReported = true;
        }

        return Status.ERROR;
    }


    @Override
    public Status update( String table, String key, Map<String, ByteIterator> values ) {
        return Status.NOT_IMPLEMENTED;
    }


    @Override
    public Status delete( String table, String key ) {
        return Status.NOT_IMPLEMENTED;
    }


    @Override
    public Status scan( String table, String startKey, int recordCount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result ) {
        return Status.NOT_IMPLEMENTED;
    }


    @Override
    public void cleanup() throws DBException {
        if ( connection != null ) {
            try {
                connection.close();
            } catch ( SQLException e ) {
                throw new DBException( e );
            }
        }
    }

}
