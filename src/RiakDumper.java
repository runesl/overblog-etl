import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.response.FetchResponse;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.Map;

public class RiakDumper implements Runnable{
    Logger logger = Logger.getLogger(getClass());
        RiakClient riakClient = null;
        final AccessLogLoader.Batch batch;
        final String aKey;
		final String bucketName;
		final String fileName;

        public RiakDumper(String url, String bucketName, AccessLogLoader.Batch batch, String fileName) {
			this.bucketName = bucketName;
            this.riakClient = new RiakClient(url);
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

                for (Map.Entry<String, FourHBlogStats> blogStats : batch) {
					final FourHBlogStats stats = blogStats.getValue();
                    String day = blogStats.getKey().substring(0, 10);
                    String blogId = blogStats.getKey().substring(14);
                    String key = day + "_" + blogId;
                    String siteId = blogStats.getValue().siteId;

                    FetchResponse fetched = riakClient.fetch(bucketName, key);
                    JSONObject json;
                    RiakObject riakObject;
                    if (fetched.hasObject()){
                        json = new JSONObject(fetched.getObject().getValue());
                        riakObject = fetched.getObject();
                    }
                    else{
                        json = new JSONObject();
                        riakObject = new RiakObject(bucketName, key);
                    }
                    json.put(fileName, stats.toJson());
                    riakObject.setValue(json.toString());
                    RequestMeta requestMeta = new RequestMeta();
                    requestMeta.setHeader("x-riak-index-blog-day_bin", blogId + "_" + day);
                    requestMeta.setHeader("x-riak-index-site-day_bin", siteId + "_" + day);
                    riakClient.store(riakObject, requestMeta);
                }
            } catch (Exception e) {
                e.printStackTrace();
           }
    }
}