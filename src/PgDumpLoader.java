import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakObject;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PgDumpLoader {

    String[] headers2 = {"day","idblog ","idelem ","type   ","hits   ","uniques","title  ","url"};
    private static int threads;
    static ThreadPoolExecutor threadPoolExecutor;

    static SimpleDateFormat in = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static SimpleDateFormat out = new SimpleDateFormat("yyyy-MM-dd-HH");
    static String bucketName;
    private static Date currentTimeStamp, parsedTimestamp;
    static BlogStats blogStats = new BlogStats();
    static private int fileType;
    public static final int FILETYPE_ACCESS_LOG = 0;
    public static final int FILETYPE_PG_DUMP = 1;
    public static String[] headers;
    static Date start = new Date();
    public static final int BATCH_SIZE = 5000;
    
    public static int totalDumped = 0;

    public static void main(String[] args) throws Exception{
        if (args.length != 5){
            System.out.println("usage: Main riakurl bucketName filename filetype(1=accesslog,2=pgdump) threads");
            System.exit(0);
        }
        fileType = Integer.parseInt(args[3]);
        threads = Integer.parseInt(args[4]);
        threadPoolExecutor = new ThreadPoolExecutor(threads, threads, 1, TimeUnit.DAYS, new ArrayBlockingQueue<Runnable>(threads));
        String url = args[0];
        File inputFile = new File(args[2]);
        bucketName = args[1];
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        int lines = 0;
        if (fileType == FILETYPE_PG_DUMP)
            headers = br.readLine().split(";");
        while (br.ready()){
            lines++;
            String line = br.readLine();
            processLine(line);
            if (lines >= BATCH_SIZE){
                while(threadPoolExecutor.getQueue().size() >= threads -1)
                    Thread.sleep(1000);
                threadPoolExecutor.execute(new RiakDumper(url, blogStats));
                currentTimeStamp = parsedTimestamp;
                lines = 0;
                blogStats = new BlogStats();
            }
        }
    }

    private static boolean processLine(String line) throws Exception{
       /* if (fileType == FILETYPE_ACCESS_LOG)
            return processLineAccesslog(line);
        else*/ if (fileType == FILETYPE_PG_DUMP)
            return processLinePgDump(line);
        throw new RuntimeException("Unkown file type: " + fileType);
    }

    private static boolean processLinePgDump(String line) {
        try {
            String[] split = line.split(";");
            String day = split[1];
            String blogId = split[3];
            Blog blog = blogStats.getBlog(blogId + "_" + day);
            blog.values = split;
            return Math.random() >0.9998;
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }
    }
                             /*
    private static boolean processLineAccesslog(String line) throws Exception{

        String[] tokens = line.split("\t");
        parsedTimestamp = getCurrentTimeStamp(currentTimeStamp, tokens[0]);
        if (currentTimeStamp == null)
            currentTimeStamp = parsedTimestamp;
        String blogID = tokens[1];
        String ip = tokens[5];
        Blog blog = blogStats.getBlog(blogID + "_" + out.format(currentTimeStamp));
        blog.values = tokens;
        return parsedTimestamp.after(currentTimeStamp);
    }
                               */
    public static class RiakDumper implements Runnable{

        RiakClient riakClient;
        BlogStats blogStats;
        String firstKey;

        public RiakDumper(String url, BlogStats blogStats) {
            firstKey = blogStats.blogs.keySet().iterator().next();
            this.riakClient = new RiakClient(url);
            this.blogStats = blogStats;
        }


         private void dumpToRiak() {
            try {
                if (blogStats.blogs.size() ==0)
                    return;
                int count = 0;
                Date t0 = new Date();
                System.out.println("dumping " + blogStats.blogs.size() + " blogstats into riak. First key: " + firstKey);
                for (String blogId : blogStats.blogs.keySet()){
                    count++;
                    Blog blog = blogStats.blogs.get(blogId);

                    RiakObject riakObject = new RiakObject(bucketName, blogId);
                    String json = serialize(blog);
                    if (count % 1000 == 0){
                        long millis = System.currentTimeMillis() - start.getTime();
                        System.out.println("dumped: " + count + " blogs. firstKey: " + firstKey + " totalDumped: " + totalDumped + " totalSecs: " + millis/1000 + " prsec " + (1000*totalDumped / millis));
                    }

                    riakObject.setValue(json);
                    riakObject.setContentType("application/json");
                    riakClient.store(riakObject);
                    totalDumped++;
                }
                long elapsed = System.currentTimeMillis() - t0.getTime();
                System.out.println("Done dumping data into riak. TooK: " + elapsed + " milliseconds. records pr. sec: " + 1000* (blogStats.blogs.size() / elapsed));
                blogStats.blogs = new HashMap<String, Blog>();
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        public void run() {
            dumpToRiak();
        }
    }
                                               /*
    private static String serializeWithIp(Blog blog) {
        String s = "{\"page_views\": " + blog.pageviews + ", \"ips\": [";
        boolean first = true;
        for (String ip : blog.ips) {
            if (first)
                first = false;
            else
            s+=", ";
            s += ip;
        }
        s+="]}";
        return s;
    }
                                                 */
    private static String serialize(Blog blog) throws Exception{
        JSONObject jsonObject = new JSONObject();
        int rowNum =0;
        for (String value : blog.values) {
            jsonObject.put(headers[rowNum], value);
            rowNum++;
        }
        return jsonObject.toString();
    }


    private static Date getCurrentTimeStamp(Date currentTimeStamp, String tstmp) throws Exception{
        Date parsed = in.parse(tstmp);
        if (currentTimeStamp == null || parsed.after(currentTimeStamp)){
            Calendar instance = Calendar.getInstance();
            instance.setTime(parsed);
            instance.set(Calendar.MINUTE, 0);
            instance.set(Calendar.SECOND, 0);
            int hours = instance.get(Calendar.HOUR);
//            System.out.println("hours" + hours);
            hours = (hours/4)*4;
//            System.out.println("hours" + hours);
            instance.set(Calendar.HOUR, hours);
            return instance.getTime();
        }
        
        return currentTimeStamp;
    }


    public static class BlogStats{
        public Map<String, Blog> blogs = new HashMap<String, Blog>();

        public Blog getBlog(String blogID) {
            Blog blog = blogs.get(blogID);
            if (blog == null){
                blog = new Blog();
                blogs.put(blogID,  blog);
            }
            return blog;
                    
        }
    }
    
    public static class Blog{
//        public Set<String> ips = new HashSet<String>();
        public String[] values;
    }
    

}

