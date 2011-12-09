import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.Map;

public class RiakDumperNew implements Runnable{
    Logger logger = Logger.getLogger(getClass());
        IRiakClient riakClient = null;
        final AccessLogLoader.Batch batch;
        final String aKey;
		final String bucketName;
		final String fileName;

        public RiakDumperNew(String url, String bucketName, AccessLogLoader.Batch batch, String fileName) {
			this.bucketName = bucketName;
            try {
                this.riakClient = RiakFactory.pbcClient();
            } catch (RiakException e) {
            }
            this.batch = batch;
            this.fileName = fileName;
            aKey = batch.size()>0 ? batch.get(0).getKey() : null; // This gets us a random key...
        }

        public void run() {
			int size = batch.size();
			logger.warn("Batch '" + aKey + "' started. size=" + size);
			long t0 = System.currentTimeMillis();
            dumpToRiak();
			long elapsed = System.currentTimeMillis() - t0+1;
            logger.warn("Batch '" + aKey + "' done. size=" + size + ", time=" + elapsed + " ms, rate=" + (1000l * size / elapsed) + "/s");
        }

		private void dumpToRiak() {
            try { 
                if (batch.size() == 0) return;

                for (Map.Entry<String, FourHBlogStats> e : batch) {
					final FourHBlogStats stats = e.getValue();
                    String day = e.getKey().substring(0, 10);
                    String blogId = e.getKey().substring(14);
                    String key = day + "_" + blogId;
                    String siteId = e.getValue().siteId;
                    Bucket fetchBucket = riakClient.fetchBucket(bucketName).execute();
                    IRiakObject fetched = fetchBucket.fetch(key).execute();
                    JSONObject json;
                    if (fetched != null){
                        json = new JSONObject(fetched.getValueAsString());
                        json.put(fileName, stats.toJson());
                        fetched.setValue(json.toString());
                        fetchBucket.store(fetched).returnBody(false).execute();
                    }
                    else{
                        Bucket bucket = riakClient.createBucket(bucketName).execute();
                        json = new JSONObject();
                        json.put(fileName, stats.toJson());
                        RiakObjectBuilder objectBuilder = RiakObjectBuilder.newBuilder(bucketName, key);
                        objectBuilder.addIndex("blog-day_bin", blogId + "_" + day);
                        objectBuilder.addIndex("site-day_bin", siteId + "_" + day);
                        objectBuilder.withValue(json.toString());

                        IRiakObject iRiakObject = objectBuilder.build();
                        bucket.store(iRiakObject).returnBody(false).execute();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
           }
    }
}