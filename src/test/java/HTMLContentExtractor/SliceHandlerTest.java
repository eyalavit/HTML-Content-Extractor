package HTMLContentExtractor;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eyal avital on 12/20/2017.
 */
public class SliceHandlerTest extends TestCase {

        public void test(){
        /**
         *testing simple insert to cassandra
         */
        CassandraConnection cassandraConnection = new CassandraConnection();
        SliceHandler sliceHandler =
                new SliceHandler("test1" , "slice1",1, cassandraConnection);
        sliceHandler.run();
        Clause indClause = QueryBuilder.eq("url", "test1");
        Select.Where query = QueryBuilder.select()
                .from("voyagerlabs", "slices")
                .where(indClause);

        ResultSet result = cassandraConnection.getSession().execute(query);
        Row row = result.one();
        assertEquals(row.toString(),"Row[test1, 1, slice1]");

        /**
        *testing  bad insert to cassandra
        */
            SliceHandler sliceHandler2 =
                    new SliceHandler(null , null,1, cassandraConnection);
            sliceHandler2.run();

    }
    public void test2(){
        /**
         * more complex insert to cassandra
         */
        CassandraConnection cassandraConnection = new CassandraConnection();
        SliceHandler sliceHandler1 =
                new SliceHandler("test1" , "slice1",1, cassandraConnection);
        SliceHandler sliceHandler2 =
                new SliceHandler("test1" , "slice2",2, cassandraConnection);
        SliceHandler sliceHandler3 =
                new SliceHandler("test1" , "slice1",3, cassandraConnection);
        sliceHandler1.run();
        sliceHandler2.run();
        sliceHandler3.run();
        Clause indClause = QueryBuilder.eq("url", "test1");
        Select.Where query = QueryBuilder.select()
                .from("voyagerlabs", "slices")
                .where(indClause);
        List<String> compreArr = new ArrayList();
        compreArr.add("Row[test1, 1, slice1]");
        compreArr.add("Row[test1, 2, slice2]");
        compreArr.add("Row[test1, 3, slice1]");
        ResultSet result = cassandraConnection.getSession().execute(query);
        List<Row> rows = result.all();
        for (Row row : rows){
            assertTrue(compreArr.contains(row.toString()));
        }
    }



}