package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;


public class CreateMetadata extends QueryBuilder {

    private final String store;


    public CreateMetadata( String store ) {
        this.store = store;
    }


    @Override
    public Query getNewQuery() {
        return new CreateMetadataQuery( this.store );
    }


    private static class CreateMetadataQuery extends Query {

        private final String store;


        CreateMetadataQuery( String store ) {
            super( false );
            this.store = store;
        }


        @Override
        public String getSql() {
            return "CREATE TABLE knn_metadata (id integer NOT NULL, textdata VARCHAR(100), PRIMARY KEY (id)) ON STORE \"" + this.store + "\"";
        }


        @Override
        public HttpRequest<?> getRest() {
            return null;
        }

        // TODO JS: Implement getCottontail()

    }

}
