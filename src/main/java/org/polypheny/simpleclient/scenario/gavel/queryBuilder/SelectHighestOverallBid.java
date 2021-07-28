package org.polypheny.simpleclient.scenario.gavel.queryBuilder;

import java.util.Map;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryView;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;

public class SelectHighestOverallBid extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;
    private final QueryView queryView;


    public SelectHighestOverallBid( QueryView queryView ) {
        this.queryView = queryView;
    }


    @Override
    public Query getNewQuery() {
        return new SelectHighestOverallBidQuery( queryView );
    }


    private static class SelectHighestOverallBidQuery extends Query {

        private final QueryView queryView;


        public SelectHighestOverallBidQuery( QueryView queryView ) {
            super( EXPECT_RESULT );
            this.queryView = queryView;
        }


        @Override
        public String getSql() {

            if ( queryView.equals( QueryView.MATERIALIZED ) ) {
                return "SELECT * FROM highestBid_materialized";
            } else if ( queryView.equals( QueryView.VIEW ) ) {
                return "SELECT * FROM highestBid_view";
            } else {
                return "SELECT last_name, first_name "
                        + "FROM \"user\" "
                        + "WHERE \"user\".id = (SELECT highest.highestUser FROM (SELECT bid.\"user\" as highestUser, MAX( bid.amount) "
                        + "FROM public.bid "
                        + "GROUP BY bid.\"user\" "
                        + "ORDER BY MAX( bid.amount) DESC) as highest Limit 1)";
            }
        }


        @Override
        public String getParameterizedSqlQuery() {
            return getSql();
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            return null;
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;

            /*
            if ( queryView.equals( QueryView.VIEW ) ) {
                return Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/public.highestBid_view" )
                        .queryString( "_limit", 100 );
            } else if ( queryView.equals( QueryView.MATERIALIZED ) ) {
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

