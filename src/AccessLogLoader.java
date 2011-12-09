import org.apache.log4j.Logger;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AccessLogLoader {
    // Constants:
    static final int BATCH_SIZE = 100;
    static final String IN_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    static final String OUT_4H_FORMAT = "yyyy-MM-dd-HH";
    static final String OUT_DATE_FORMAT = "yyyy-MM-dd";

    public static void main(String[] args) throws Exception {
        //---------- Parse arguments:
        if (args.length != 4){
            System.err.println("Expected parameters: riakurl bucketName filename threadcount");
            System.exit(1);
        }
        final String url = args[0];
        final String bucketName = args[1];
        final File inputFile = new File(args[2]);
        final int threads = Integer.parseInt(args[3]);

        //---------- Then go to work:
        AccessLogLoader aclolo = new AccessLogLoader(threads);
        aclolo.processAccessLog(url, bucketName, inputFile);
    }

    //========== Instance state: ========================================
    private static Logger logger = Logger.getLogger(AccessLogLoader.class);

    BlogStatCollection blogStats = new BlogStatCollection();
    final ThreadPoolExecutor tpe;

    public static final SimpleDateFormat in_fmt = new SimpleDateFormat(IN_DATE_FORMAT);
    public static final SimpleDateFormat out_fmt = new SimpleDateFormat(OUT_DATE_FORMAT);
    public static final SimpleDateFormat out_4h_fmt = new SimpleDateFormat(OUT_4H_FORMAT);

    public AccessLogLoader(int threads) {
        tpe = new ThreadPoolExecutor(threads, threads, 1, TimeUnit.DAYS, new ArrayBlockingQueue<Runnable>(threads));
    }

    void processAccessLog(String url, String bucketName, File input) throws IOException {
        final BufferedReader br = new BufferedReader(new FileReader(input));
        int total_size = 0;
        long t0 = System.currentTimeMillis();

        String line;
        while ((line=br.readLine()) != null) {
            AccessLine accessLine;
            try {
                accessLine = AccessLine.parse(line);
                blogStats.update(accessLine);

            } catch (Throwable t) {
                logger.warn("Cannot parse line: " + line +" ; reason: "+t);
            }
        }
        logger.warn("Read all data. Starting to dump");
        startBatches(url, bucketName, blogStats, input.getName());
        total_size += blogStats.size();

        log("Final batch started.  Waiting for started tasks to complete.");

        waitUntilDone();
        long elapsed = System.currentTimeMillis() - t0;
        log("Done dumping data into riak. Stored " + total_size +" records in " + elapsed + " ms. "+
                " Store rate: " + 1000*(total_size / elapsed));
    }

    private void startBatches(String url, String bucketName, BlogStatCollection blogStats, String fileName) {
        List<Batch> batches = blogStats.batch(BATCH_SIZE);
        for (Batch batch : batches) {
            startBatch(new RiakDumper(url, bucketName, batch, fileName));
        }
    }

    private void startBatch(Runnable job) {
        do {
            try {
                tpe.execute(job);
                break; // Success.
            } catch (RejectedExecutionException ree) {
                // Failure - queue was full.  Sleep and retry.
                sleepABit();
            }
        } while (true);
    }

    private void sleepABit() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
            /*ignore*/
        }
    }

    private void waitUntilDone() {
        tpe.shutdown();
        boolean done;
        do {
            try {
                done = tpe.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException ie) {
                done = false;
            }
        } while (!done);
    }

    // State for reporting progress:
    final Date start = new Date();
    final long start_millis = System.currentTimeMillis();
    public int totalRecords = 0, totalBatches = 0;
    synchronized void addTotalProgress(int additionalRecords) {
        totalBatches++; totalRecords += additionalRecords;

        long elapsed = System.currentTimeMillis() - start_millis;
        log("Progress: elapsed=" + (elapsed/1000) + "s; "+
                " batches="+totalBatches+" (avg_batch_rate="+(1000*totalBatches/elapsed)+"/s);"+
                " records="+totalRecords+" (avg_record_rate="+(1000*totalRecords/elapsed)+"/s);");
    }



    public void log(String msg) {
        logger.info(msg);
    }


    private static Date roundTo4HTimestamp(String tstmp) {
        Date parsed = null;
        try {
            parsed = in_fmt.parse(tstmp);
        } catch (ParseException e) {
            logger.warn(e);
        }
        Calendar instance = Calendar.getInstance();
        instance.setTime(parsed);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        int hours = instance.get(Calendar.HOUR);
        hours = (hours/4)*4;
        instance.set(Calendar.HOUR, hours);
        return instance.getTime();
    }


    public static class BlogStatCollection {
        private final Map<String, FourHBlogStats> blogs = new HashMap<String, FourHBlogStats>();

        public FourHBlogStats getFourHBlogStats(String key) {
            FourHBlogStats blog = blogs.get(key);
            if (blog == null){
                blog = new FourHBlogStats();
                blogs.put(key, blog);
            }
            return blog;
        }

        public int size() { return blogs.size(); }

        public void clearBlogs() { blogs.clear(); }

        public void update(AccessLine accessLine) {
            FourHBlogStats fourHBlogStats = getFourHBlogStats(out_4h_fmt.format(roundTo4HTimestamp(accessLine.timestamp)) + "_" + accessLine.blogId);
            fourHBlogStats.addAccess(accessLine);
        }

        public List<Batch> batch(int batchSize) {
            List<Batch> result = new ArrayList<Batch>();
            Batch cur_batch = new Batch();

            for (Map.Entry<String, FourHBlogStats> e : blogs.entrySet()) {
                if (cur_batch.size() >= batchSize) {
                    result.add(cur_batch);
                    cur_batch = new Batch();
                }
                cur_batch.add(e);
            }
            if (cur_batch.size() > 0)
                result.add(cur_batch);
            return result;
        }
    }

    public static class Batch extends ArrayList<Map.Entry<String, FourHBlogStats>> {}

}

