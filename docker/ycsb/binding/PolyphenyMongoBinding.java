/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-30.10.26, 00:45 The Polypheny Project
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


"""
inspired by structure of: https://github.com/brianfrankcooper/YCSB/blob/master/mongodb/src/main/java/site/ycsb/db/MongoDbClient.java
"""

package org.polypheny.simpleclient.scenario.ycsb.binding;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.multimodel.DocumentResult;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.Result;
import org.polypheny.jdbc.multimodel.ScalarResult;
import org.polypheny.jdbc.types.PolyDocument;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.measurements.Measurements;

public class PolyphenyMongoBinding extends DB {

    private static final char[] JSON_HEX = "0123456789abcdef".toCharArray();
    private static final Pattern IDENTIFIER = Pattern.compile( "[A-Za-z_][A-Za-z0-9_]*" );

    private Connection connection;
    private String namespace;
    private boolean errorReported;
    private boolean serverProjection;
    private int batchSize = 1;
    private final List<String> pendingInserts = new ArrayList<>();
    private String pendingCollection;
    private Exception insertFailure;
    private int readsPerTransaction = 1;
    private int pendingReads;
    private Exception readFailure;

    @Override
    public void init() throws DBException {
        Properties properties = getProperties();
        namespace = properties.getProperty( "db.namespace", "ycsb" );
        serverProjection = Boolean.parseBoolean( properties.getProperty( "db.serverProjection", "false" ) );

        try {
            int configuredBatchSize = Integer.parseInt( properties.getProperty( "db.batchsize", "100" ) );
            if ( configuredBatchSize < 1 ) {
                throw new IllegalArgumentException( "db.batchsize must be positive" );
            }
            batchSize = Boolean.parseBoolean( properties.getProperty( "dotransactions", "true" ) )
                    ? 1 : configuredBatchSize;
            if ( Boolean.parseBoolean( properties.getProperty( "dotransactions", "true" ) ) ) {
                readsPerTransaction = Integer.parseInt( properties.getProperty( "db.readsPerTransaction", "1" ) );
                if ( readsPerTransaction < 1 ) {
                    throw new IllegalArgumentException( "db.readsPerTransaction must be positive" );
                }
                if ( readsPerTransaction > 1 && !isReadOnlyWorkload( properties ) ) {
                    throw new IllegalArgumentException( "Grouped reads require a read-only CoreWorkload (workload C)" );
                }
            }
            Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );

            connection = DriverManager.getConnection(
                    properties.getProperty( "db.url" ),
                    properties.getProperty( "db.user", "pa" ),
                    properties.getProperty( "db.passwd", "" )
            );

            connection.setAutoCommit( readsPerTransaction == 1 );

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
        if ( insertFailure != null ) {
            return Status.ERROR;
        }
        try {
            String target = collection( table );
            String valuesJson = valuesJson( values );
            String document = "{\"_id\":" + quoteMongoString( key )
                    + (values.isEmpty() ? "}" : "," + valuesJson.substring( 1 ));
            if ( !pendingInserts.isEmpty() && !target.equals( pendingCollection ) ) {
                flushInserts();
            }
            pendingCollection = target;
            pendingInserts.add( document );
            if ( pendingInserts.size() >= batchSize ) {
                flushInserts();
            }
            return Status.OK;
        } catch ( Exception e ) {
            // Do not retry an ambiguously committed batch during cleanup.
            insertFailure = e;
            return reportError( e );
        }
    }

    private void flushInserts() throws SQLException {
        if ( pendingInserts.isEmpty() ) {
            return;
        }
        try ( var statement = new QueryStatement( connection.unwrap( PolyConnection.class ) ) ) {
            String query = batchSize == 1
                    ? pendingCollection + ".insertOne(" + pendingInserts.get( 0 ) + ")"
                    : pendingCollection + ".insertMany([" + String.join( ",", pendingInserts ) + "])";
            long inserted = getUpdateCount( statement.execute( namespace, "mongo", query ) );
            if ( inserted != pendingInserts.size() ) {
                throw new SQLException( "Expected " + pendingInserts.size() + " inserted documents, got " + inserted );
            }
        }
        pendingInserts.clear();
    }

    @Override
    public Status read( String table, String key, Set<String> fields, Map<String, ByteIterator> result ) {
        if ( readFailure != null ) {
            return Status.ERROR;
        }
        try {
            Status status = readOne( table, key, fields, result );
            if ( readsPerTransaction > 1 ) {
                if ( status != Status.OK ) {
                    abortReads( new SQLException( "Could not read key: " + key ) );
                } else if ( ++pendingReads == readsPerTransaction ) {
                    connection.commit();
                    pendingReads = 0;
                }
            }
            return status;
        } catch ( Exception e ) {
            if ( readsPerTransaction > 1 ) {
                abortReads( e );
            }
            return reportError( e );
        }
    }

    private static boolean isReadOnlyWorkload( Properties properties ) {
        return "site.ycsb.workloads.CoreWorkload".equals( properties.getProperty( "workload" ) )
                && Double.parseDouble( properties.getProperty( "readproportion", "0.95" ) ) == 1
                && Double.parseDouble( properties.getProperty( "updateproportion", "0.05" ) ) == 0
                && List.of( "insertproportion", "scanproportion", "readmodifywriteproportion", "deleteproportion" )
                .stream().allMatch( key -> Double.parseDouble( properties.getProperty( key, "0" ) ) == 0 );
    }

    private void abortReads( Exception failure ) {
        readFailure = failure;
        pendingReads = 0;
        try {
            connection.rollback();
        } catch ( SQLException rollbackError ) {
            failure.addSuppressed( rollbackError );
        }
    }

    private Status readOne( String table, String key, Set<String> fields, Map<String, ByteIterator> result ) throws SQLException {
        try ( var statement = new QueryStatement( connection.unwrap( PolyConnection.class ) ) ) {
            String query = collection( table ) + ".find({\"_id\":" + quoteMongoString( key ) + "}"
                    + (serverProjection && fields != null ? "," + projection( fields ) : "")
                    + ").limit(2)";

            DocumentResult documents = statement.execute( namespace, "mongo", query )
                    .unwrap( DocumentResult.class );

            boolean found = false;
            for ( PolyDocument document : documents ) {
                if ( found ) {
                    throw new IllegalStateException( "Multiple documents found for key: " + key );
                }

                fillMap( result, document, fields );
                found = true;
            }

            return found ? Status.OK : Status.NOT_FOUND;
        }
    }

    @Override
    public Status update( String table, String key, Map<String, ByteIterator> values ) {
        try ( var statement = new QueryStatement( connection.unwrap( PolyConnection.class ) ) ) {
            String query = collection( table ) + ".updateOne({\"_id\":"
                    + quoteMongoString( key ) + "},{\"$set\":"
                    + valuesJson( values ) + "})";

            long updated = getUpdateCount(
                    statement.execute( namespace, "mongo", query )
            );

            return updated == 0 || updated == 1 ? Status.OK : Status.ERROR;

        } catch ( Exception e ) {
            return reportError( e );
        }
    }

    @Override
    public Status delete( String table, String key ) {
        try ( var statement = new QueryStatement( connection.unwrap( PolyConnection.class ) ) ) {
            String query = collection( table ) + ".deleteOne({\"_id\":" + quoteMongoString( key ) + "})";
            long deleted = getUpdateCount( statement.execute( namespace, "mongo", query ) );
            if ( deleted == 1 ) {
                return Status.OK;
            }
            if ( deleted == 0 ) {
                return Status.NOT_FOUND;
            }
            return Status.ERROR;
        } catch ( Exception e ) {
            return reportError( e );
        }
    }

    @Override
    public Status scan( String table, String startKey, int recordCount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result ) {

        if ( recordCount < 1 ) {
            return Status.ERROR;
        }

        try ( var statement = new QueryStatement( connection.unwrap( PolyConnection.class ) ) ) {
            String query = collection( table ) + ".aggregate(["
                    + "{\"$match\":{\"_id\":{\"$gte\":" + quoteMongoString( startKey ) + "}}},"
                    + "{\"$sort\":{\"_id\":1}},"
                    + "{\"$limit\":" + recordCount + "}"
                    + (serverProjection && fields != null
                    ? ",{\"$project\":" + projection( fields ) + "}" : "")
                    + "])";

            DocumentResult documents = statement.execute( namespace, "mongo", query )
                    .unwrap( DocumentResult.class );

            for ( PolyDocument document : documents ) {
                HashMap<String, ByteIterator> row = new HashMap<>();
                fillMap( row, document, fields );
                result.add( row );
            }

            return Status.OK;

        } catch ( Exception e ) {
            return reportError( e );
        }
    }

    @Override
    public void cleanup() throws DBException {
        try ( Connection workerConnection = connection ) {
            if ( readFailure != null ) {
                throw readFailure;
            }
            if ( insertFailure != null ) {
                throw insertFailure;
            }
            flushInserts();
            commitPendingReads();
        } catch ( Exception failure ) {
            // YCSB logs cleanup exceptions but still exports results.
            if ( readsPerTransaction > 1 ) {
                Measurements.getMeasurements().reportStatus( "CLEANUP", Status.ERROR );
            }
            throw new DBException( failure );
        }
    }

    private void commitPendingReads() throws SQLException {
        if ( pendingReads == 0 ) {
            return;
        }
        try {
            connection.commit();
            pendingReads = 0;
        } catch ( SQLException | RuntimeException failure ) {
            abortReads( failure );
            throw failure;
        }
    }

    private static void fillMap( Map<String, ByteIterator> result, PolyDocument document, Set<String> fields ) throws SQLException {
        if ( fields == null ) {
            for ( var entry : document.entrySet() ) {
                if ( !"_id".equals( entry.getKey() ) ) {
                    result.put( entry.getKey(), new ByteArrayByteIterator(
                            entry.getValue().asString().getBytes( StandardCharsets.UTF_8 ) ) );
                }
            }
        } else {
            for ( String field : fields ) {
                var value = document.get( field );
                if ( value == null || "_id".equals( field ) ) {
                    throw new IllegalStateException( "Document does not contain requested field: " + field );
                }
                result.put( field, new ByteArrayByteIterator( value.asString().getBytes( StandardCharsets.UTF_8 ) ) );
            }
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
                    throw new IllegalStateException( "Write result has no updateCount: " + document );
                }

                count += updateCount.asLong();
            }

            return count;
        }

        throw new IllegalStateException( "Unexpected write result: " + response.getClass().getName() );
    }

    private static String valuesJson( Map<String, ByteIterator> values ) {
        StringBuilder json = new StringBuilder( "{" );
        for ( var entry : values.entrySet() ) {
            if ( json.length() > 1 ) {
                json.append( ',' );
            }
            json.append( quoteMongoString( entry.getKey() ) ).append( ':' )
                    .append( quoteMongoString( entry.getValue().toString() ) );
        }
        return json.append( '}' ).toString();
    }

    private static String projection( Set<String> fields ) {
        if ( fields.isEmpty() ) {
            return "{\"_id\":1}";
        }
        StringBuilder json = new StringBuilder( "{" );
        for ( String field : fields ) {
            if ( json.length() > 1 ) {
                json.append( ',' );
            }
            json.append( quoteMongoString( field ) ).append( ":1" );
        }
        return json.append( '}' ).toString();
    }

    private static String quoteMongoString( String value ) {
        StringBuilder result = new StringBuilder( value.length() + 2 ).append( '"' );
        int plainRun = 0;
        for ( int i = 0; i < value.length(); i++ ) {
            char c = value.charAt( i );
            boolean plain = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9') || c == '_' || c == ' ';
            if ( plain && plainRun < 64 ) {
                result.append( c );
                plainRun++;
            } else {
                plainRun = 0;
                result.append( '\\' ).append( 'u' )
                        .append( JSON_HEX[(c >>> 12) & 15] ).append( JSON_HEX[(c >>> 8) & 15] )
                        .append( JSON_HEX[(c >>> 4) & 15] ).append( JSON_HEX[c & 15] );
            }
        }
        return result.append( '"' ).toString();
    }

    private static String collection( String table ) {
        if ( table == null || !IDENTIFIER.matcher( table ).matches() ) {
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

    private static class QueryStatement extends PolyStatement implements AutoCloseable {
        QueryStatement( PolyConnection connection ) {
            super( connection );
        }

        @Override
        public void close() throws SQLException {
            if ( getStatementId() > 0 ) {
                PolyConnection connection = getConnection();
                connection.getPrismInterfaceClient().closeStatement( getStatementId(), connection.getTimeout() );
            }
        }
    }
}
