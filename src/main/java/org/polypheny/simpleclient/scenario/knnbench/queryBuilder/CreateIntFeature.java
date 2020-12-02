package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class CreateIntFeature extends QueryBuilder {

    private final String store;
    private final int dimension;


    public CreateIntFeature( String store, int dimension ) {
        this.store = store;
        this.dimension = dimension;
    }


    @Override
    public Query getNewQuery() {
        return new CreateIntFeatureQuery( store, dimension );
    }


    private static class CreateIntFeatureQuery extends Query {

        private final String store;
        private final int dimension;


        CreateIntFeatureQuery( String store, int dimension ) {
            super( false );
            this.store = store;
            this.dimension = dimension;
        }


        @Override
        public String getSql() {
            return "CREATE TABLE knn_intfeature (id INTEGER NOT NULL, feature INTEGER ARRAY(1, " + this.dimension + "), PRIMARY KEY(id)) ON STORE \"" + this.store + "\"";
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }

        // TODO JS: Implement getCottontail()

    }

}
