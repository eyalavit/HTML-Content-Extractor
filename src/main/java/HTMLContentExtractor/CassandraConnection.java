package HTMLContentExtractor;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Eyal avital on 12/18/2017.
 */
public class CassandraConnection {
    private Cluster cluster;
    private Session session;
    public static final Logger LOG = LoggerFactory.getLogger(CassandraConnection.class);

    public CassandraConnection() {
        initializeCluster();
    }

    /**
     * initialize Cluster and session
     */
    private void initializeCluster() {
        try {
            PoolingOptions poolingOptions = new PoolingOptions();
           poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 4);
            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 10);
            poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, 32768);
            poolingOptions.setHeartbeatIntervalSeconds(60);

            this.cluster = Cluster.builder()
                    // TODO: Extract this IP address to a file
                    .addContactPoint("127.0.0.1").withPort(9042)
                    .withPoolingOptions(poolingOptions)
                    .withProtocolVersion(ProtocolVersion.V3)
                    .build();
            this.session = this.cluster.connect();
            initDB();

        }catch (Exception e){
            LOG.error("initialize cluster failed, we exit now");
            System.exit(1);
        }
    }

    /**
     * create keyspace named voyagerLabs and
     * create table is voyagerLabs keyspace name slices
     */
    private void initDB() {
        session.execute("CREATE KEYSPACE IF NOT EXISTS voyagerLabs\n" +
                "    WITH replication = {'class': 'SimpleStrategy',\n" +
                "                        'replication_factor' : 3}");
        session.execute("use voyagerLabs");
        session.execute("CREATE TABLE IF NOT EXISTS slices ("
                + "url varchar,"
                + "slice text,"
                + "sliceNumber varint,"
                + "PRIMARY KEY (url, sliceNumber))");
    }

    public Session getSession() {
        return this.session;
    }

    /**
     * stop connection to cassandra
     */
    public void shutdown() {
        if (this.cluster != null)
            this.cluster.close();
    }
}
