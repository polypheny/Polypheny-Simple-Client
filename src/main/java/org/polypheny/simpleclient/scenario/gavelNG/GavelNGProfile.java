package org.polypheny.simpleclient.scenario.gavelNG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.simpleclient.Pair;

@Slf4j
public class GavelNGProfile {


    public final Queue<Pair<Pair<QueryPossibility, Integer>, Integer>> timeline;
    public final Queue<QueryPossibility> warmUp;
    public final List<Pair<String, String>> tableStores = new ArrayList<>();
    public final List<Pair<String, String>> factoryStores = new ArrayList<>();


    public GavelNGProfile( Properties properties ) {
        timeline = castProfileTimeline( properties.getProperty( "schedules" ) );
        warmUp = castWarmUp( properties.getProperty( "warmup" ) );
        selectStore( properties.getProperty( "storeForFactory" ), properties.getProperty( "storeForTable" ) );
    }


    public GavelNGProfile( Map<String, String> cdl ) {
        timeline = castProfileTimeline( cdl.get( "schedules" ) );
        warmUp = castWarmUp( cdl.get( "warmup" ) );
        selectStore( cdl.get( "storeForFactory" ), cdl.get( "storeForTable" ) );
    }


    private void selectStore( String storeForFactory, String storeForTable ) {
        if ( !Objects.equals( storeForTable, "" ) ) {
            String[] selectedStores = storeForTable.replace( "\"", "" ).split( "," );
            for ( String selectedStore : selectedStores ) {
                tableStores.add( new Pair<>( selectedStore.split( "-" )[0], selectedStore.split( "-" )[1] ) );
            }
        } else if ( !Objects.equals( storeForFactory, "" ) ) {
            String[] selectedStores = storeForFactory.replace( "\"", "" ).split( "," );
            for ( String selectedStore : selectedStores ) {
                factoryStores.add( new Pair<>( selectedStore.split( "-" )[0], selectedStore.split( "-" )[1] ) );
            }
        } else {
            log.warn( "No particular Store selected for the table creation." );
        }
    }


    private Queue<QueryPossibility> castWarmUp( String warmup ) {
        Queue<QueryPossibility> warmUp = new LinkedList<>();
        String[] parts = warmup.replace( "\"", "" ).split( "," );

        for ( String part : parts ) {
            QueryPossibility query = getQueryPossibility( part );
            warmUp.add( query );
        }
        return warmUp;
    }


    private Queue<Pair<Pair<QueryPossibility, Integer>, Integer>> castProfileTimeline( String profileTimeline ) {
        Queue<Pair<Pair<QueryPossibility, Integer>, Integer>> castedTimeline = new LinkedList<>();

        String[] parts = profileTimeline.replace( "\"", "" ).split( "," );

        for ( String part : parts ) {

            QueryPossibility query = getQueryPossibility( part );

            String withoutLanguage = part.split( "-" )[1];
            castedTimeline.add( new Pair<>( new Pair<>( query, Integer.parseInt( withoutLanguage.split( "d" )[0].substring( 1 ) ) ), Integer.parseInt( withoutLanguage.split( "d" )[1] ) ) );
        }

        return castedTimeline;
    }


    @NotNull
    private QueryPossibility getQueryPossibility( String part ) {
        QueryPossibility query;

        if ( part.startsWith( "sql-" ) ) {
            switch ( part.split( "-" )[1].substring( 0, 1 ) ) {
                case "i":
                    query = QueryPossibility.INSERT_SQL;
                    break;
                case "s":
                    query = QueryPossibility.SIMPLE_SELECT_SQL;
                    break;
                case "c":
                    query = QueryPossibility.COMPLEX_SELECT_SQL;
                    break;
                case "u":
                    query = QueryPossibility.UPDATE_SQL;
                    break;
                case "d":
                    query = QueryPossibility.DELETE_SQL;
                    break;
                case "t":
                    query = QueryPossibility.TRUNCATE_SQL;
                    break;
                default:
                    log.warn( "Please check how to write a Scenario, this letter is not possible to use." );
                    throw new RuntimeException( "Please check how to write a Scenario, this letter is not possible to use." );
            }

        } else if ( part.startsWith( "mql-" ) ) {
            switch ( part.split( "-" )[1].substring( 0, 1 ) ) {
                case "i":
                    query = QueryPossibility.INSERT_MQL;
                    break;
                case "s":
                    query = QueryPossibility.SIMPLE_SELECT_MQL;
                    break;
                case "c":
                    query = QueryPossibility.COMPLEX_SELECT_MQL;
                    break;
                case "u":
                    query = QueryPossibility.UPDATE_MQL;
                    break;
                case "d":
                    query = QueryPossibility.DELETE_MQL;
                    break;
                case "t":
                    query = QueryPossibility.TRUNCATE_MQL;
                    break;
                default:
                    log.warn( "Please check how to write a Scenario, this letter is not possible to use." );
                    throw new RuntimeException( "Please check how to write a Scenario, this letter is not possible to use." );
            }
        } else {
            log.warn( "Only possible to use MQL or SQL." );
            throw new RuntimeException( "Only possible to use MQL or SQL." );
        }
        return query;
    }


    enum QueryPossibility {
        INSERT_SQL(1), SIMPLE_SELECT_SQL(2), COMPLEX_SELECT_SQL(3), UPDATE_SQL(4), DELETE_SQL(5), TRUNCATE_SQL(6),
        INSERT_MQL(7), SIMPLE_SELECT_MQL(8), COMPLEX_SELECT_MQL(9), UPDATE_MQL(10), DELETE_MQL(11), TRUNCATE_MQL(12);


        public int id;
        QueryPossibility( int id ) {
            this.id = id;
        }

        public static Map<Integer, String> getQueryTypes(){
            Map<Integer, String> queryTypes = new HashMap<>();
            for(QueryPossibility queryPossibility: values()){
                queryTypes.put( queryPossibility.id, queryPossibility.name() );
            }
            return queryTypes;
        }


        public static String getById( Integer templateId ) {
            for(QueryPossibility queryPossibility: values()){
                if(queryPossibility.id == templateId){
                    return queryPossibility.name();
                }
            }
            return "unknown";
        }
    }

}
