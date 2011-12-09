import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class FourHBlogStats {
    public Set<String> ips = new HashSet<String>();
    public int hits = 0;
    public String siteId;


    public FourHBlogStats() {
    }

    public void addAccess(AccessLine line) {
        ips.add(line.ip);
        hits++;
        siteId = line.siteId;
    }

    public JSONObject toJson() throws JSONException {
        JSONObject root = new JSONObject();
        root.put("hits", hits);

        JSONArray ip_list = new JSONArray();
        for (String ip : ips)
            ip_list.put(ip);
        root.put("ips", ip_list);
        return root;
    }

}
