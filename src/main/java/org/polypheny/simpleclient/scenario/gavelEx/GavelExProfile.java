package org.polypheny.simpleclient.scenario.gavelEx;

import com.sun.tools.javac.util.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertBid;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertCategory;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertPicture;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertRandomAuction;
import org.polypheny.simpleclient.scenario.gavel.queryBuilder.InsertRandomBid;

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

            switch ( part.substring( 0, 1 ) ){
                case "i":
                    query = QueryPossibility.INSERT;
                    break;
                case "s":
                    query = QueryPossibility.SIMPLE_SELECT;
                    break;
                case "a":
                    query = QueryPossibility.AVERAGE_SELECT;
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
                default:
                    log.warn("Please check how to write a Scenario, this letter is not possible to use."  );
                    throw new RuntimeException("Please check how to write a Scenario, this letter is not possible to use.");
            }



            castedTimeline.add( new Pair<>( new Pair<>( query, Integer.parseInt( part.substring( 1, 2 ) ) ), Integer.parseInt( part.split( "d" )[1] ) ) );
            log.warn( part.substring( 0, 1 ) );
            log.warn( part.substring( 1, 2 ) );
            log.warn( part.split( "d" )[1] );
        }

        return castedTimeline;
    }

    enum QueryPossibility {
        INSERT, SIMPLE_SELECT, AVERAGE_SELECT, COMPLEX_SELECT, UPDATE, DELETE;

        public final List<Class> insertQueries = Arrays.asList( InsertRandomAuction.class, InsertRandomBid.class );
        public final List<Class> updateQueries = new ArrayList<>();
        public final List<Class> simpleSelectQueries = new ArrayList<>();
        public final List<Class> averageSelectQueries = new ArrayList<>();
        public final List<Class> complexSelectQueries = new ArrayList<>();

        public List<Class> getPossibleClasses(){
            switch ( this ){
                case INSERT:
                    return insertQueries;
                case UPDATE:
                    return updateQueries;
                case SIMPLE_SELECT:
                    return simpleSelectQueries;
                case AVERAGE_SELECT:
                    return averageSelectQueries;
                case COMPLEX_SELECT:
                    return complexSelectQueries;
                default:
                    throw new RuntimeException("This QueryPossibility has no saved Queries. Please add a List of Classes with suitable queries.");
            }
        }



    }

}
