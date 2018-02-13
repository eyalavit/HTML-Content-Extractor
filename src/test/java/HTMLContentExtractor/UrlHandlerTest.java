package HTMLContentExtractor;

import junit.framework.TestCase;
/**
 * Created by Eyal avital on 12/20/2017.
 */
public class UrlHandlerTest extends TestCase {
    /**
     * testing bad Url input
     */
    public void testBadUrlInput() {
        CassandraConnection cassandraConnection = new CassandraConnection();
        UrlHandler urlHandler = new UrlHandler("bad url!", cassandraConnection);
        urlHandler.run();
    }

    /**
     * testing cassandra null connection input
     */
    public void testCassandraConnection() {
        CassandraConnection cassandraConnection = null;
        UrlHandler urlHandler = new UrlHandler("https://www.ynet.co.il", cassandraConnection);
        urlHandler.run();
    }
}

