package HTMLContentExtractor;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Eyal avital on 12/17/2017.
 */
public class Main {

    public static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        String[] urls = initUrls("input.json");
        CassandraConnection cassandraConnection = new CassandraConnection();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (String url : urls) {
            Runnable sliceMacker = new UrlHandler(url, cassandraConnection);
            executor.execute(sliceMacker);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (Exception e) {
            LOG.error("executor await Termination fail");
        }
        cassandraConnection.shutdown();
        LOG.info("cassandra Connection shut down");
        LOG.info("end of program");
    }


    /**
     * @param input name of a JSON file that include a list of urls
     * @return list of urls
     */
    private static String[] initUrls(String input) {
        Gson gson = new Gson();
        Input inputFile = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader((String) input));
            inputFile = gson.fromJson(br, Input.class);
        } catch (FileNotFoundException e) {
            LOG.error("can't find input file!!! will now exit");
            System.exit(1);
        }
        return inputFile.getUrls();
    }
}


