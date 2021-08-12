package org.polypheny.simpleclient.scenario.gavel.queryBuilder;

import java.util.HashMap;
import java.util.Map;
import kong.unirest.HttpRequest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.QueryMode;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;

public class SelectPriceBetweenAndNotInCategory extends QueryBuilder {

    public static final boolean EXPECT_RESULT = true;
    private final QueryMode queryMode;


    public SelectPriceBetweenAndNotInCategory( QueryMode queryMode ) {
        this.queryMode = queryMode;
    }


    @Override
    public Query getNewQuery() {
        return new SelectPriceBetweenAndNotInCategoryQuery( queryMode );
    }


    private static class SelectPriceBetweenAndNotInCategoryQuery extends Query {

        private final QueryMode queryMode;


        public SelectPriceBetweenAndNotInCategoryQuery( QueryMode queryMode ) {
            super( EXPECT_RESULT );
            this.queryMode = queryMode;
        }


        @Override
        public String getSql() {
            if ( queryMode.equals( QueryMode.MATERIALIZED ) ) {
                return "SELECT * FROM priceBetween_materialized LIMIT 100";
            } else if ( queryMode.equals( QueryMode.VIEW ) ) {
                return "SELECT * FROM priceBetween_view LIMIT 100";
            } else {
                return "SELECT auction.title, bid.amount "
                        + "FROM auction, category, bid "
                        + "WHERE bid.auction = auction.id "
                        + "AND bid.amount > 1000 AND bid.amount < 1000000 "
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


        @Override
        public String getMongoQl() {
            // $lookup is not supported // substitute query
            return "db.bid.aggregate(["
                    + "{\"$match\":{\"$or\":[{\"amount\":{\"$gt\": 1000}}, {\"amount\":{\"$lt\": 1000000}}]}}, "
                    + "{\"$sort\":{\"amount\": -1 }}, "
                    + "{\"$limit\":100}"
                    + "])";
        }

    }

}
