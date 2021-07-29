package org.polypheny.simpleclient.scenario.gavel.queryBuilder;

import java.util.HashMap;
import java.util.Map;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;

public class SelectHighestOverallBid extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;
    private final QueryMode queryMode;


    public SelectHighestOverallBid( QueryMode queryMode ) {
        this.queryMode = queryMode;
    }


    @Override
    public Query getNewQuery() {
        return new SelectHighestOverallBidQuery( queryMode );
    }


    private static class SelectHighestOverallBidQuery extends Query {

        private final QueryMode queryMode;


        public SelectHighestOverallBidQuery( QueryMode queryMode ) {
            super( EXPECT_RESULT );
            this.queryMode = queryMode;
        }


        @Override
        public String getSql() {
            if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                return "SELECT * FROM highestBid_materialized LIMIT 1";
            } else if ( queryMode.equals( QueryMode.VIEW ) ) {
                return "SELECT * FROM highestBid_view LIMIT 1";
            } else {
                return "SELECT last_name, first_name "
                        + "FROM \"user\" "
                        + "WHERE \"user\".id = (SELECT highest.highestUser FROM (SELECT bid.\"user\" as highestUser, MAX( bid.amount) "
                        + "FROM public.bid "
                        + "GROUP BY bid.\"user\" "
                        + "ORDER BY MAX( bid.amount) DESC) as highest Limit 1) LIMIT 1";
            }
        }


        @Override
        public String getParameterizedSqlQuery() {
            return getSql();
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            return new HashMap<>();
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;

            /*
            if ( queryMode.equals( QueryMode.VIEW ) ) {
                return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.highestBid_view" )
                        .queryString( "_limit", 100 );
            } else if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.highestBid_materialized" )
                        .queryString( "_limit", 100 );
            } else {
                return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.user" )
                        .queryString( "_project", "public.user.last_name,public.user.first_name")
                        .queryString( "public.bid.user", "=" + "public.user.id" )
                        .queryString( "public.bid.auction", "=" + "public.auction.id" )
                        .queryString( "public.picture.auction", "=" + "public.auction.id" )
                        .queryString( "public.auction.user", "=" + "public.user.id" )
                        .queryString( "public.auction.category", "=" + "public.category.id" )
                        .queryString( "_limit", 100 );
            }
             */
        }

    }

}

