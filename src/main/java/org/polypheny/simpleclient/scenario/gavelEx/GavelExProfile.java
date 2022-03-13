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

            switch ( part.substring( 0, 1 ) ) {
                case "i":
                    query = QueryPossibility.INSERT;
                    break;
                case "s":
                    query = QueryPossibility.SIMPLE_SELECT;
                    break;
                case "c":
                    query = QueryPossibility.COMPLEX_SELECT;
                    break;
                case "u":
                    query = QueryPossibility.UPDATE;
                    break;
                case "d":
                    query = QueryPossibility.DELETE;
                    break;
                case "t":
                    query = QueryPossibility.TRUNCATE;
                    break;
                default:
                    log.warn( "Please check how to write a Scenario, this letter is not possible to use." );
                    throw new RuntimeException( "Please check how to write a Scenario, this letter is not possible to use." );
            }

            castedTimeline.add( new Pair<>( new Pair<>( query, Integer.parseInt( part.substring( 1, 2 ) ) ), Integer.parseInt( part.split( "d" )[1] ) ) );
        }

        return castedTimeline;
    }


    enum QueryPossibility {
        INSERT, SIMPLE_SELECT, COMPLEX_SELECT, UPDATE, DELETE, TRUNCATE;


    }

}
