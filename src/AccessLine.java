import java.util.List;

public class AccessLine {
    public final String blogId;
    public final String ip;
    public final String url;
    public final String title;
    public final String ref;
    private final List<String> keywords;
    public final String timestamp;
    public final String siteId;

    public static AccessLine parse(String line) {
		return new AccessLine(line);
	}

    private AccessLine(String line){
        String[] tokens = line.split("\t");
        this.timestamp = tokens[0].substring(tokens[0].indexOf("logger:") + 8);
        this.blogId = tokens[1];
        this.siteId = tokens[2];
        this.url = tokens[9];
        this.ref = tokens[4];
        this.title = tokens[8];
        this.ip = tokens[5];
		this.keywords = null; //TODO
    }
}
