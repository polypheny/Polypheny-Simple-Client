package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class CreateRealFeature extends QueryBuilder {

    private final String store;
    private final int dimension;


    public CreateRealFeature( String store, int dimension ) {
        this.store = store;
        this.dimension = dimension;
    }


    @Override
    public Query getNewQuery() {
        return new CreateRealFeatureQuery( store, dimension );
    }


    private static class CreateRealFeatureQuery extends Query {

        private final String store;
        private final int dimension;


        CreateRealFeatureQuery( String store, int dimension ) {
            super( false );
            this.store = store;
            this.dimension = dimension;
        }


        @Override
        public String getSql() {
            return "CREATE TABLE knn_realfeature (id INTEGER NOT NULL, feature REAL ARRAY(1, " + this.dimension + "), PRIMARY KEY(id)) ON STORE \"" + this.store + "\"";
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }

    }

}
