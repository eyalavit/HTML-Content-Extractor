package HTMLContentExtractor;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Eyal avital on 12/19/2017.
 *
 * sliceHander - insert one slice to cassandra
 */
public class SliceHandler implements Runnable {
    private String url;
    private String slice;
    private int sliceNumber;
    private CassandraConnection cassandraConnection;
    public static final Logger LOG = LoggerFactory.getLogger(SliceHandler.class);

    public SliceHandler(String url, String slice, int sliceNumber, CassandraConnection cassandraConnection) {
        this.url = url;
        this.slice = slice;
        this.sliceNumber = sliceNumber;
        this.cassandraConnection = cassandraConnection;
    }

    @Override
    public void run() {
            if ((this.url != null)&(this.slice != null)) {
                Insert query = QueryBuilder.insertInto("slices")
                        .value("url", this.url)
                        .value("slice", this.slice)
                        .value("slicenumber", this.sliceNumber);
                if (cassandraConnection != null) {
                    cassandraConnection.getSession().execute(query);
                } else {
                    LOG.error("bad cassandra connection, will exit now");
                    System.exit(1);
                }
            }else{
                LOG.error("bad url or slice");
            }
        }
    }

