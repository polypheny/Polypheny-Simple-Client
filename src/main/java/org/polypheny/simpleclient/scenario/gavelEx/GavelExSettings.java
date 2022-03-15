package org.polypheny.simpleclient.scenario.gavelEx;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.Pair;
import org.polypheny.simpleclient.executor.Executor.ExecutorFactory;
import org.polypheny.simpleclient.executor.ExecutorException;
import org.polypheny.simpleclient.executor.PolyphenyDbExecutor;

@Slf4j
public class GavelExSettings {

    List<Pair<String, String>> tableStores = new ArrayList<>();
    List<Pair<String, String>> factoryStores = new ArrayList<>();

    public GavelExSettings( Properties properties, ExecutorFactory executorFactory ) {
        List<String> dataStore = Arrays.asList( properties.getProperty( "dataStores" ).replace( "\"", "" ).split( "," ) );
        depolySelectedStore( executorFactory, dataStore );
        selectStore( properties );
    }


    private void selectStore( Properties properties ) {
        String storeForFactory = properties.getProperty( "storeForFactory" );
        String storeForTable = properties.getProperty( "storeForTable" );

        if ( !Objects.equals( storeForTable, "" ) ) {
            String[] selectedStores = storeForTable.replace( "\"", "" ).split( "," );
            for ( String selectedStore : selectedStores ) {
                tableStores.add( new Pair<>( selectedStore.split( "-" )[0], selectedStore.split( "-" )[1] ));
            }
        } else if ( !Objects.equals( storeForFactory, "" ) ) {
            String[] selectedStores = storeForFactory.replace( "\"", "" ).split( "," );
            for ( String selectedStore : selectedStores ) {
                factoryStores.add( new Pair<>( selectedStore.split( "-" )[0], selectedStore.split( "-" )[1] ));
            }
        }else{
            log.warn( "No particular Store selected for the table creation." );
        }


    }


    private static void depolySelectedStore( ExecutorFactory executorFactory, List<String> dataStores ) {

        PolyphenyDbExecutor executor = (PolyphenyDbExecutor) executorFactory.createExecutorInstance();
        try {
            // Remove hsqldb store
            executor.dropStore( "hsqldb" );
            // Deploy stores
            for ( String store : dataStores ) {
                switch ( store ) {
                    case "hsqldb":
                        executor.deployHsqldb();
                        break;
                    case "postgres":
                        executor.deployPostgres( true );
                        break;
                    case "monetdb":
                        executor.deployMonetDb( true );
                        break;
                    case "cassandra":
                        executor.deployCassandra( true );
                        break;
                    case "file":
                        executor.deployFileStore();
                        break;
                    case "cottontail":
                        executor.deployCottontail();
                        break;
                    case "mongodb":
                        executor.deployMongoDb();
                        break;
                    default:
                        throw new RuntimeException( "Unknown data store: " + store );
                }
            }
            executor.executeCommit();
        } catch ( ExecutorException e ) {
            throw new RuntimeException( "Exception while configuring stores", e );
        } finally {
            try {
                executor.closeConnection();
            } catch ( ExecutorException e ) {
                log.error( "Exception while closing connection", e );
            }
        }
    }


}
