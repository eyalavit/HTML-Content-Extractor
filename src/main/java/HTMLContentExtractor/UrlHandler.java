package HTMLContentExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Eyal avital on 12/17/2017.
 *
 * urlHandler insert html content to cassandra
 * by creating sliceHandlers for every slice that he finds
 */
public class UrlHandler implements Runnable {
    private String url;
    private CassandraConnection cassandraConnection;
    private int SLICESIZE;
    public static final Logger LOG = LoggerFactory.getLogger(UrlHandler.class);

    public UrlHandler(String url, CassandraConnection cassandraConnection) {
        this.url = url;
        this.cassandraConnection = cassandraConnection;
        this.SLICESIZE = 1000;
    }

    public void run() {
        BufferedReader br = getBufferedReaderFromSliceUrl();
        if (br == null) {
            LOG.error("bad url, " + this.url + ", return buffer equals null");
        } else {
            StringBuilder htmlContent = new StringBuilder();
            try {
                String line;
                while ((line = br.readLine()) != null) {
                    htmlContent.append(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            ArrayList<String> stringArrayList = splitHtml(htmlContent.toString(), this.SLICESIZE);
            ExecutorService executor = Executors.newFixedThreadPool(4);
            int sliceNumber = 0;
            for (String insert : stringArrayList) {
                Runnable sliceHandelr = new SliceHandler(this.url, insert, sliceNumber, this.cassandraConnection);
                executor.execute(sliceHandelr);
                sliceNumber++;
            }
            executor.shutdown();
            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    /**
     * @param string include all the content of the html page in a huge string
     * @return the html content sliced to picecs that every
     * slice is at length of SLICESIZE.
     */
    private ArrayList<String> splitHtml(String string, int SLICESIZE) {
        if (SLICESIZE > 2500) {
            LOG.error("SLICESIZE cant be more then 10KB, we now exit");
            System.exit(1);
        }
        ArrayList<String> stringArrayList = new ArrayList<String>();
        int index = 0;
        int sliceLength;
        while (index < string.length()) {
            if (string.length() - index < SLICESIZE) {
                sliceLength = string.length() - index;
            } else {
                sliceLength = SLICESIZE;
            }
            stringArrayList.add(string.substring(index, index + sliceLength));
            index = index + sliceLength;
        }
        return stringArrayList;
    }

    /**
     * @return BufferedReader for this.url
     */
    private BufferedReader getBufferedReaderFromSliceUrl() {
        URL sliceUrl;
        InputStream is;
        BufferedReader br = null;
        try {
            // Make a URL to the web page
            sliceUrl = new URL(this.url);
            // Get the input stream through URL Connection
            URLConnection con = sliceUrl.openConnection();
            is = con.getInputStream();
            br = new BufferedReader(new InputStreamReader(is));
        } catch (Exception e) {
            LOG.error("cant read from URL" + this.url);
        }
        return br;
    }
}


