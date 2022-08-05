/**
 * 
 */
package uk.bl.wap.crawler.h3.frontier;

import org.apache.commons.httpclient.URIException;
import org.archive.crawler.prefetch.FrontierPreparer;
import org.archive.crawler.spring.SheetOverlaysManager;
import org.archive.modules.CrawlURI;
import org.archive.net.UURIFactory;

import uk.bl.wap.crawler.frontier.RedisSimplifiedFrontier;

/**
 * 
 * Purpose of this class is to run it standalone and check the working.
 * 
 * Needs a suitable Redis instance.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SimplifiedFrontierAdaptorStandaloneRunner {

    /**
     * @param args
     * @throws URIException
     */
    public static void main(String[] args) throws URIException {
    	RedisSimplifiedFrontier rsf = new RedisSimplifiedFrontier();
        rsf.setEndpoint("redis://localhost:6379");
        rsf.connect();

    	SimplifiedFrontierAdaptor rf = new SimplifiedFrontierAdaptor();
        rf.setSimplifiedFrontier(rsf);        
        rf.setSheetOverlaysManager(new SheetOverlaysManager());

        FrontierPreparer fp = new FrontierPreparer();

        // CrawlURI curi = new CrawlURI(u, pathFromSeed, via, viaContext );
        CrawlURI curi = new CrawlURI(
                UURIFactory.getInstance("http://example.org"));
        // Set delay to 10ms for testing...
        curi.setPolitenessDelay(10);

        fp.prepare(curi);

        rf.processScheduleAlways(curi);

        CrawlURI nextCuri = rf.findEligibleURI();

        if (nextCuri.equals(curi)) {
            System.out.println("Got "+nextCuri);
            System.out.println("They match!");
            rf.processFinish(curi);
        }

        nextCuri = rf.findEligibleURI();
        rf.processFinish(curi);

        rsf.dequeue(curi.getClassKey(), curi.getURI());

        rsf.retireQueue(curi.getClassKey());
    }

}
