package org.polypheny.simpleclient.scenario.knnbench.queryBuilder;


import java.util.ArrayList;
import java.util.List;
import kong.unirest.HttpRequest;
import org.polypheny.simpleclient.query.CottontailQuery;
import org.polypheny.simpleclient.query.CottontailQuery.QueryType;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.EntityDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Schema;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Type;


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


        @Override
        public CottontailQuery getCottontail() {
            List<ColumnDefinition> columns = new ArrayList<>();
            columns.add( ColumnDefinition.newBuilder().setName( "id" ).setType( Type.INTEGER ).build() );
            columns.add( ColumnDefinition.newBuilder().setName( "feature" ).setType( Type.INT_VEC ).setLength( dimension ).build() );
            EntityDefinition entityDefinition = EntityDefinition.newBuilder()
                    .setEntity( Entity.newBuilder().setSchema( Schema.newBuilder().setName( "public" ).build() ).setName( "knn_intfeature" ).build() )
                    .addAllColumns( columns )
                    .build();
            return new CottontailQuery(
                    QueryType.ENTITY_CREATE,
                    entityDefinition
            );
        }
    }

}
