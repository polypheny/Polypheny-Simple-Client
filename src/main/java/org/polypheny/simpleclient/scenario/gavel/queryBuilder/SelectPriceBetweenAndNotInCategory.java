package org.polypheny.simpleclient.scenario.gavel.queryBuilder;

import java.util.HashMap;
import java.util.Map;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryView;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;

public class SelectPriceBetweenAndNotInCategory extends QueryBuilder {

    public static final boolean EXPECT_RESULT = true;
    private final QueryView queryView;


    public SelectPriceBetweenAndNotInCategory( QueryView queryView ) {
        this.queryView = queryView;
    }


    @Override
    public Query getNewQuery() {
        return new SelectPriceBetweenAndNotInCategoryQuery( queryView );
    }


    private static class SelectPriceBetweenAndNotInCategoryQuery extends Query {

        private final QueryView queryView;


        public SelectPriceBetweenAndNotInCategoryQuery( QueryView queryView ) {
            super( EXPECT_RESULT );
            this.queryView = queryView;
        }


        @Override
        public String getSql() {
            if ( queryView.equals( QueryView.MATERIALIZED ) ) {
                return "SELECT * FROM priceBetween_materialized LIMIT 100";
            } else if ( queryView.equals( QueryView.VIEW ) ) {
                return "SELECT * FROM priceBetween_view LIMIT 100";
            } else {
                return "SELECT auction.title, bid.amount "
                        + "FROM auction, category, bid "
                        + "WHERE bid.auction = auction.id "
                        + "AND auction.category = category.id "
                        + "AND bid.amount > 1000 AND bid.amount < 1000000 "
                        + "AND not exists ( SELECT category.name "
                        + "FROM category WHERE category.name in ('Travel', 'Stamps', 'Motors')) "
                        + "ORDER BY bid.amount DESC LIMIT 100";
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
        }


    }

}
