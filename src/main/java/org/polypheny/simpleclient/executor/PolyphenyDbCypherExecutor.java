package org.polypheny.simpleclient.executor;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.main.CsvWriter;
import org.polypheny.simpleclient.query.BatchableInsert;
import org.polypheny.simpleclient.query.MultipartInsert;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.RawQuery;
import org.polypheny.simpleclient.scenario.AbstractConfig;

@Slf4j
public class PolyphenyDbCypherExecutor extends PolyphenyDbHttpExecutor {


    public PolyphenyDbCypherExecutor( String host, CsvWriter csvWriter ) {
        super( "Cypher", Query::getCypher, host, csvWriter );
    }


    @Override
    protected HttpRequest<?> buildQuery( String query ) {
        JsonObject data = new JsonObject();
        data.addProperty( "query", query );
        data.addProperty( "database", "test" );

        return Unirest.post( "{protocol}://{host}:{port}/cypher" )
                .header( "Content-Type", "application/json" )
                .body( data );

    }


    @Override
    public void executeCommit() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeRollback() throws ExecutorException {
        log.error( "Unsupported operation: Rollback" );
    }


    @Override
    public void closeConnection() throws ExecutorException {
        // NoOp
    }


    @Override
    public void executeInsertList( List<BatchableInsert> batchList, AbstractConfig config ) throws ExecutorException {
        String currentTable = null;
        List<String> rows = new ArrayList<>();
        for ( BatchableInsert query : batchList ) {
            query.debug();
            if ( query instanceof MultipartInsert ) {
                continue;
            }
            if ( currentTable == null ) {
                currentTable = query.getTable();
            }

            if ( currentTable.equals( query.getTable() ) ) {
                rows.add( Objects.requireNonNull( query.getCypherRowExpression() ) );
            } else {
                throw new RuntimeException( "Different tables in multi-inserts. This should not happen!" );
            }
        }
        if ( rows.size() > 0 ) {
            executeQuery( new RawQuery( null, null, Query.buildCypherManyInsert( currentTable, rows ), false ) );
        }
    }


    public static class PolyphenyDbCypherExecutorFactory extends ExecutorFactory {

        private final String host;


        public PolyphenyDbCypherExecutorFactory( String host ) {
            this.host = host;
        }


        @Override
        public PolyphenyDbCypherExecutor createExecutorInstance( CsvWriter csvWriter ) {
            return new PolyphenyDbCypherExecutor( host, csvWriter );
        }


        @Override
        public int getMaxNumberOfThreads() {
            return 0;
        }

    }

}
