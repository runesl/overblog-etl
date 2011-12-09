import static org.junit.Assert.assertEquals;

public class AccessLogLineParserTest {


    @org.junit.Test
    public void testParseLine() throws Exception {
        String accessLine = "Dec  1 10:34:12 jfg-riak1 logger: 2011-11-28 19:10:00\t1876159\t1\t0\thttp://passiondecuisine.over-blog.com/archive-11-2011.html\t41.137.25.230\t1\t17\tطريقة سهلة لإعداد القديد والخليع بدون شمس بدون شحمة مع هدى جنان - Le blog de Sanfoura مدونة السنفورة\thttp://passiondecuisine.over-blog.com/article-89855450.html";
        AccessLine line = AccessLine.parse(accessLine);
        assertEquals("2011-11-28 19:10:00", line.timestamp);
        assertEquals("1876159", line.blogId);
        assertEquals("1", line.siteId);
        assertEquals("41.137.25.230", line.ip);
        assertEquals("طريقة سهلة لإعداد القديد والخليع بدون شمس بدون شحمة مع هدى جنان - Le blog de Sanfoura مدونة السنفورة", line.title);
        assertEquals("http://passiondecuisine.over-blog.com/article-89855450.html", line.url);
    }
    @org.junit.Test
    public void testParseLineWithKeyWord() throws Exception {
        String accessLine = "Dec  1 10:34:12 jfg-riak1 logger: 2011-11-28 19:10:00\t14907\t1\t0\thttp://www.google.be/imgres?q=peinture surréaliste&um=1&hl=fr&sa=N&biw=1280&bih=657&tbm=isch&tbnid=89eIzudMkSUMcM:&imgrefurl=http://peinturesurrealiste.over-blog.com/categorie-25685.html&docid=p4APCvOne-QhyM&imgurl=http://idata.over-blog.com/0/01/49/07/atelier_2.jpg&w=500&h=333&ei=ZMvTTr7oA4Pm-gb_st3MDg&zoom=1&iact=hc&vpx=596&vpy=195&dur=2562&hovh=183&hovw=275&tx=172&ty=126&sig=104519151782777138845&page=7&tbnh=136&tbnw=190&start=129&ndsp=18&ved=1t:429,r:15,s:129\t81.243.80.70\t1\t18\tPeinture surréaliste - contact et explications - Son atelier - Yannick et Mienne… - Explication de ses… - Surréalisme… - Peintre contemporain surrealiste - YANNICK GLEVAREC (ou) des ANGES aux DEMONS\thttp://peinturesurrealiste.over-blog.com/categorie-25685.html\tpeinture surréaliste";
        AccessLine line = AccessLine.parse(accessLine);
    }

}
