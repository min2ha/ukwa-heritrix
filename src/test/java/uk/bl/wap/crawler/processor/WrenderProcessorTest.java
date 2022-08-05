package uk.bl.wap.crawler.processor;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.archive.modules.CrawlURI;
import org.archive.net.UURIFactory;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

/**
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WrenderProcessorTest {

    /**
     * 
     * @throws JSONException
     * @throws IOException
     */
    @Test
    public void testProcessHar() throws JSONException, IOException {
        // String wrenderUrl =
        // "http://127.0.0.1:8000/render?url=http://example.com/&warc_prefix=wrender";

        CrawlURI curi = new CrawlURI(
                UURIFactory.getInstance("http://example.com/"));

        // WrenderProcessor wp = new WrenderProcessor();
        // wp.process(curi);

        File wrjson = new File("src/test/resources/wrender-example.json");
        JSONObject har = WrenderProcessor
                .readJsonFromUrl(wrjson.toURI().toURL(), 5, 5);
        WrenderProcessor.processJson(har, curi);

        assertEquals("Status code was not found!",
                200,
                curi.getFetchStatus());
        assertEquals("CrawlURIs not as expected!", 1,
                curi.getOutLinks().size());
        CrawlURI l = curi.getOutLinks().iterator().next();
        assertEquals("Can't find expected outlink!",
                "https://www.iana.org/domains/example", l.getURI());

    }

    /**
     * Another test that has SVG-style hrefs in it.
     * 
     * @throws JSONException
     * @throws IOException
     */
    @Test
    public void testProcessHarWithSvgHrefs() throws JSONException, IOException {

        CrawlURI curi = new CrawlURI(
                UURIFactory.getInstance("http://www.thisismoney.co.uk/"));

        File wrjson = new File(
                "src/test/resources/wrender-svg-hrefs-example.json");
        JSONObject har = WrenderProcessor
                .readJsonFromUrl(wrjson.toURI().toURL(), 5, 5);
        WrenderProcessor.processJson(har, curi);

        // This is a redirect, which is not directly recorded in the
        // request/response chain:
        assertEquals("Status code was not found!", 200, curi.getFetchStatus());
        assertEquals("CrawlURIs not as expected!", 333,
                curi.getOutLinks().size());
        // Extract links into a simple String array:
        List<String> links = new ArrayList<String>();
        for (CrawlURI l : curi.getOutLinks()) {
            links.add(l.getURI());
        }
        // Check and outlink was there:
        boolean found = links.contains(
                "https://www.thisismoney.co.uk/money/podcast/article-8875705/Has-V-shaped-recovery-turned-double-dip-Money-podcast.html");
        assertEquals("Can't find expected outlink!", true, found);

    }

}
