package org.polypheny.simpleclient.scenario.gavel.queryBuilder;

import java.util.Map;
import kong.unirest.HttpRequest;
import kong.unirest.Unirest;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.polypheny.simpleclient.query.Query;
import org.polypheny.simpleclient.query.QueryBuilder;

public class SelectComplexView extends QueryBuilder {

    private static final boolean EXPECT_RESULT = true;
    private final boolean queryView;

    public SelectComplexView(boolean queryView){
        this.queryView = queryView;
    }

    @Override
    public Query getNewQuery(){
        return new SelectComplexQuery(queryView);
    }



    private static class SelectComplexQuery extends Query{

        private final boolean queryView;

        public SelectComplexQuery(boolean queryView){
            super(EXPECT_RESULT);
            this.queryView = queryView;
        }


        @Override
        public String getSql() {
            return "";
            /*
            if(queryView){

            }else {

            }

             */
        }


        @Override
        public String getParameterizedSqlQuery() {
            return "";
            /*
            if(queryView){

            }else {

            }

             */
        }


        @Override
        public Map<Integer, ImmutablePair<DataTypes, Object>> getParameterValues() {
            return null;
        }


        @Override
        public HttpRequest<?> getRest() {
            return Unirest.get( "" );
            }
            /*
            if(queryView){

            }else {

            }

             */
        }

    }

