package org.polypheny.simpleclient.scenario.gavelEx;

import com.sun.tools.javac.util.Pair;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GavelExProfile {


    public final Queue<Pair<Pair<QueryPossibility, Integer>, Integer>> timeline;


    public GavelExProfile( Properties properties ) {

        String profileTimeline = properties.getProperty( "schedules" );

        timeline = castProfileTimeline( profileTimeline );
    }


    private Queue<Pair<Pair<QueryPossibility, Integer>, Integer>> castProfileTimeline( String profileTimeline ) {
        Queue<Pair<Pair<QueryPossibility, Integer>, Integer>> castedTimeline = new LinkedList<>();

        String[] parts = profileTimeline.replace( "\"", "" ).split( "," );

        for ( String part : parts ) {

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
            }else{
                log.warn( "Only possible to use MQL or SQL." );
                throw new RuntimeException( "Only possible to use MQL or SQL." );
            }

            String withoutLanguage = part.split( "-" )[1];
            castedTimeline.add( new Pair<>( new Pair<>( query, Integer.parseInt( withoutLanguage.split( "d" )[0].substring( 1 ) ) ), Integer.parseInt( withoutLanguage.split( "d" )[1] ) ) );
            log.warn( withoutLanguage.split( "d" )[0].substring( 1 ) );
        }

        return castedTimeline;
    }


    enum QueryPossibility {
        INSERT_SQL, SIMPLE_SELECT_SQL, COMPLEX_SELECT_SQL, UPDATE_SQL, DELETE_SQL, TRUNCATE_SQL,
        INSERT_MQL, SIMPLE_SELECT_MQL, COMPLEX_SELECT_MQL, UPDATE_MQL, DELETE_MQL, TRUNCATE_MQL


    }

}
