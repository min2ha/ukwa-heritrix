/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual
 *  contributors.
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package uk.bl.wap.crawler.frontier;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.archive.checkpointing.Checkpoint;
import org.archive.checkpointing.Checkpointable;
import org.archive.crawler.event.CrawlStateEvent;
import org.archive.crawler.framework.Frontier.FrontierGroup;
import org.archive.crawler.postprocessor.CandidatesProcessor;
import org.archive.crawler.spring.SheetOverlaysManager;
import org.archive.modules.CrawlURI;
import org.archive.modules.SchedulingConstants;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;
import org.archive.modules.net.CrawlHost;
import org.archive.modules.net.CrawlServer;
import org.archive.modules.net.ServerCache;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.archive.spring.KeyedProperties;
import org.archive.util.ArchiveUtils;
import org.archive.util.Reporter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.Lifecycle;

import uk.bl.wap.crawler.postprocessor.KafkaKeyedDiscardedFeed;
import uk.bl.wap.modules.deciderules.RecentlySeenDecideRule;

/**
 * Based on
 * /heritrix-contrib/src/main/java/org/archive/crawler/frontier/AMQPUrlReceiver.java
 * 
 * Example:
 * 
 * {"parentUrl": "http://crawl-test-site:4000/crawl-test-site/", "url": "http://crawl-test-site:4000/crawl-test-site/", "parentUrlMetadata": {"pathFromSeed": "", "heritableData": {"source": "", "heritable": ["source", "heritable"]}}, "headers": {}, "hop": "", "isSeed": true, "method": "GET"}
 * @contributor anjackson
 */
public class KafkaUrlReceiver
        implements Lifecycle, ApplicationContextAware,
        ApplicationListener<CrawlStateEvent>, Checkpointable, Reporter {

    @SuppressWarnings("unused")
    private static final long serialVersionUID = 2423423423894L;

    private static final Logger logger = 
            Logger.getLogger(KafkaUrlReceiver.class.getName());

    protected ApplicationContext appCtx;
    public void setApplicationContext(ApplicationContext appCtx) throws BeansException {
        this.appCtx = appCtx;
    }

    protected SheetOverlaysManager sheetOverlaysManager;

    public SheetOverlaysManager getSheetOverlaysManager() {
        return sheetOverlaysManager;
    }

    @Autowired
    public void setSheetOverlaysManager(
            SheetOverlaysManager sheetOverlaysManager) {
        this.sheetOverlaysManager = sheetOverlaysManager;
    }

    protected CandidatesProcessor candidates;
    public CandidatesProcessor getCandidates() {
        return candidates;
    }
    /**
     * Received urls are run through the supplied CandidatesProcessor, which
     * checks scope and schedules the urls. By default the crawl job's normal
     * candidates processor is autowired in, but a different one can be
     * configured if special scoping rules are desired.
     */
    @Autowired
    public void setCandidates(CandidatesProcessor candidates) {
        this.candidates = candidates;
    }

    protected ServerCache serverCache;

    public ServerCache getServerCache() {
        return this.serverCache;
    }

    @Autowired
    public void setServerCache(ServerCache serverCache) {
        this.serverCache = serverCache;
    }

    protected KafkaKeyedDiscardedFeed discardedUriFeed;

    public KafkaKeyedDiscardedFeed getDiscardedUriFeed() {
        return discardedUriFeed;
    }

    @Autowired
    public void setDiscardedUriFeed(KafkaKeyedDiscardedFeed discardedUriFeed) {
        this.discardedUriFeed = discardedUriFeed;
    }

    private boolean discardedUriFeedEnabled = false;

    public boolean isDiscardedUriFeedEnabled() {
        return discardedUriFeedEnabled;
    }

    public void setDiscardedUriFeedEnabled(boolean discardedUriFeedEnabled) {
        this.discardedUriFeedEnabled = discardedUriFeedEnabled;
    }

    protected String bootstrapServers = "localhost:9092";
    public String getBootstrapServers() {
        return this.bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    protected String groupId = "crawlers";
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    protected String topic = "uris.tocrawl";
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    private boolean seekToBeginning = true;

    public boolean isSeekToBeginning() {
        return seekToBeginning;
    }

    public void setSeekToBeginning(boolean seekToBeginning) {
        this.seekToBeginning = seekToBeginning;
    }

    private int maxPollRecords = 100;

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    protected boolean isRunning = false; 

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    private long discardedCount = 0;
    private long enqueuedCount = 0;

    // For reporting on last-known position on different partitions:
    private Map<Integer, Long> currentOffsets = new HashMap<Integer, Long>();

    /**
     * The maximum prefetch count to use, meaning the maximum number of messages
     * to be consumed without being acknowledged. Using 'null' would specify
     * there should be no upper limit (the default).
     */
    private Integer pollTimeout = 1000;

    private transient Lock lock = new ReentrantLock(true);

    public class KafkaConsumerRunner implements Runnable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer<String, byte[]> consumer;

        public KafkaConsumerRunner(boolean seekToBeginning) {
            logger.info("Setting up KafkaConsumerRunner...");
            Properties props = new Properties();
            props.put("bootstrap.servers", getBootstrapServers());
            props.put("group.id", getGroupId());
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "60000");
            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                    256 * 1024); // Default is 50MB
            props.put("max.poll.records", "" + maxPollRecords);
            props.put("auto.offset.reset", "earliest");
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer",
                    ByteArrayDeserializer.class.getName());
            consumer = new KafkaConsumer<String, byte[]>(props);
            // Subscribe:
            consumer.subscribe(Arrays.asList(getTopic()));
            // Rewind if requested:
            if (seekToBeginning) {
                logger.warning("Rewinding to the beginning of the " + getTopic()
                        + " URL queue.");
                seekToBeginning();
            } else {
                logger.info("Resuming consumption of the " + getTopic()
                        + " URL queue.");
            }

        }

        public void run() {
            logger.info("Running KafkaConsumer... :: bootstrapServers = "
                    + getBootstrapServers());
            logger.info("Running KafkaConsumer... :: topic = " + getTopic());
            logger.info(
                    "Running KafkaConsumer... :: group_id = " + getGroupId());
            // Until the end...
            try {
                long count = 0;
                // And now poll for records:
                while (!closed.get()) {
                    try {
                        ConsumerRecords<String, byte[]> records = consumer
                                .poll(pollTimeout);
                        // Handle new records
                        for (ConsumerRecord<String, byte[]> record : records) {
                            try {
                                String decodedBody = new String(record.value(),
                                        "UTF-8");
                                logger.finer("Processing crawl request: "
                                        + decodedBody);
                                JSONObject jo = new JSONObject(decodedBody);
                                processCrawlRequest(jo);

                            } catch (Exception e) {
                                logger.log(Level.SEVERE,
                                        "problem creating JSON from String received via Kafka "
                                                + record.value(),
                                        e);
                            }
                            count += 1;
                            currentOffsets.put(record.partition(),
                                    record.offset());
                            if (count % 1000 == 0) {
                                logger.info("Processed " + count
                                        + " messages so far. Last message offset="
                                        + record.offset() + " partition="
                                        + record.partition()
                                        + ". Total enqueued="
                                        + enqueuedCount + " discarded="
                                        + discardedCount);
                            }
                        }

                    } catch (WakeupException e) {
                        logger.info("Poll routine awoken for shutdown...");
                    }
                }
            } finally {
                logger.info("Closing consumer...");
                consumer.close();
                logger.info("Consumer closed.");
            }
            logger.info("Exiting KafkaConsumer.run()...");
            return;
        }

        /**
         * How we process crawl request messages.
         * 
         * @param jo
         */
        private void processCrawlRequest(JSONObject jo) {
            if ("GET".equals(jo.getString("method"))) {
                try {
                    // Make the CrawlURI:
                    CrawlURI curi = makeCrawlUri(jo);
                    KeyedProperties.clearAllOverrideContexts();

                    // Add a seed to the crawl:
                    if (curi.isSeed()) {
                        logger.info("Adding seed to crawl: " + curi);
                        enqueuedCount++;

                        // Note that if we have already added a seed this does
                        // nothing:
                        candidates.getSeeds().addSeed(curi);

                        // Also clear any quotas if a seeds marked as forced:
                        if (curi.forceFetch()) {
                            logger.info(
                                    "Clearing down quota stats for " + curi);
                            // Group stats:
                            FrontierGroup group = candidates.getFrontier()
                                    .getGroup(curi);
                            group.getSubstats().clear();
                            group.makeDirty();
                            // By server:
                            final CrawlServer server = serverCache
                                    .getServerFor(curi.getUURI());
                            server.getSubstats().clear();
                            server.makeDirty();
                            // And by host:
                            final CrawlHost host = serverCache
                                    .getHostFor(curi.getUURI());
                            host.getSubstats().clear();
                            host.makeDirty();
                        }
                    } else {

                        // Attempt to add this URI to the crawl:
                        logger.fine("Adding URI to crawl: " + curi + " "
                                + curi.getPathFromSeed() + " "
                                + curi.forceFetch());
                        int statusAfterCandidateChain = candidates
                                .runCandidateChain(curi, null);

                        // Only >=0 status codes get scheduled, so we can use
                        // that to log e.g.
                        // org.archive.modules.fetcher.FetchStatusCodes.S_OUT_OF_SCOPE
                        if (statusAfterCandidateChain < 0) {
                            logger.finest("Discarding URI " + curi
                                    + " with Status Code: "
                                    + statusAfterCandidateChain);
                            discardedCount++;
                            // n.b. The discarded URIs streamed out here:
                            if (discardedUriFeedEnabled) {
                                discardedUriFeed.doInnerProcess(curi);
                            }
                        } else {
                            // Was successfully enqueued:
                            enqueuedCount++;
                            if (enqueuedCount % 1000 == 0) {
                                logger.info("Sampling enqueued URLs: " + curi);
                            }
                        }
                    }
                } catch (URIException e) {
                    logger.log(Level.WARNING,
                            "problem creating CrawlURI from json received via Kafka "
                                    + jo,
                            e);
                } catch (JSONException e) {
                    logger.log(Level.SEVERE,
                            "problem creating CrawlURI from json received via Kafka "
                                    + jo,
                            e);
                } catch (Exception e) {
                    logger.log(Level.SEVERE,
                            "Unanticipated problem creating CrawlURI from json received via Kafka "
                                    + jo,
                            e);
                }

            } else {
                logger.info("ignoring url with method other than GET - " + jo);
            }

        }

        /**
         * This can be used to seek to the start of the Kafka feed after
         * subscribing:
         */
        public void seekToBeginning() {
            if (consumer.subscription().size() > 0) {
                logger.info(
                        "Seeking to the beginning... (this can take a while)");
                // Ensure partitions have been assigned by running a .poll():
                logger.info("Do a poll...");
                consumer.poll(0);
                // Reset to the start:
                logger.info("Now seek...");
                consumer.seekToBeginning(consumer.assignment());
                logger.info("Seek-to-beginning has finished.");
            }
        }

        // Shutdown hook which can be called from a separate thread
        public void shutdown() {
            closed.set(true);
            // Break out of poll() so we can shut down...
            consumer.wakeup();
        }
        
    }

    transient private KafkaConsumerRunner kafkaConsumer;
    transient private ThreadGroup kafkaProducerThreads;
    transient private ExecutorService executorService;

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        logger.info("Shutting down on STOP event...");
        this.shutdown();
    }

    private void startup() {
        lock.lock();
        try {
            if (!this.isRunning) {
                kafkaProducerThreads = new ThreadGroup(
                        Thread.currentThread().getThreadGroup().getParent(),
                        "KafkaProducerThreads");
                ThreadFactory threadFactory = new ThreadFactory() {
                    public Thread newThread(Runnable r) {
                        return new Thread(kafkaProducerThreads, r);
                    }
                };
                executorService = Executors.newFixedThreadPool(1,
                        threadFactory);

                logger.info("Requesting launch of the KafkaURLReceiver...");
                kafkaConsumer = new KafkaConsumerRunner(seekToBeginning);
                executorService.execute(kafkaConsumer);
                this.isRunning = true;

                // Only seek to the beginning at the start of the crawl:
                if (seekToBeginning) {
                    seekToBeginning = false;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void shutdown() {
        lock.lock();
        try {
            if (isRunning) {
                logger.info("Requesting shutdown of the KafkaURLReceiver...");
                this.kafkaConsumer.shutdown();
                try {
                    logger.info(
                            "Awaiting termination of the ExecutorService...");
                    this.executorService.awaitTermination(4, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE,
                            "Exception while terminating Kafka thread...", e);
                }
                logger.info("Forcing shutdown of the KafkaURLReceiver...");
                this.executorService.shutdownNow();
                isRunning = false;
                logger.info("Shutdown of the KafkaURLReceiver complete.");
            }
        } finally {
            lock.unlock();
        }
    }

    protected static final Set<String> REQUEST_HEADER_BLACKLIST = new HashSet<String>(Arrays.asList(
            "accept-encoding", "upgrade-insecure-requests", "host", "connection"));


    // {
    // "headers": {
    // "Referer": "https://archive.org/",
    // "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML,
    // like Gecko) Ubuntu Chromium/32.0.1700.102 Chrome/32.0.1700.102
    // Safari/537.36",
    // "Accept": "image/webp,*/*;q=0.8"
    // },
    // "url":
    // "https://analytics.archive.org/0.gif?server_ms=256&server_name=www19.us.archive.org&service=ao&loadtime=358&timediff=-8&locale=en-US&referrer=-&version=2&count=9",
    // "method": "GET"
    // }
    protected CrawlURI makeCrawlUri(JSONObject jo)
            throws URIException, JSONException {

        UURI uuri = UURIFactory.getInstance(jo.getString("url"));
        String viaStr = jo.getString("parentUrl");
        // Cope if we are passed an empty value by copying the URL:
        if (viaStr == null || "".equals(viaStr))
            viaStr = uuri.getURI();
        UURI via = UURIFactory.getInstance(viaStr);

        JSONObject parentUrlMetadata = jo.getJSONObject("parentUrlMetadata");
        String parentHopPath = parentUrlMetadata.getString("pathFromSeed");
        String hop = jo.optString("hop", "");
        String hopPath = parentHopPath + hop;

        CrawlURI curi = new CrawlURI(uuri, hopPath, via,
                LinkContext.INFERRED_MISC);

        populateHeritableMetadata(curi, parentUrlMetadata);

        // set the http headers from the Kafka message:
        if (jo.has("headers")) {
            JSONObject joHeaders = jo.getJSONObject("headers");
            Map<String, String> customHttpRequestHeaders = new HashMap<String, String>();
            for (Object key : joHeaders.keySet()) {
                String k = key.toString();
                if (!k.startsWith(":")
                        && !REQUEST_HEADER_BLACKLIST.contains(k)) {
                    customHttpRequestHeaders.put(k,
                            joHeaders.getString(key.toString()));
                }
            }
            curi.getData().put("customHttpRequestHeaders",
                    customHttpRequestHeaders);
        }

        // Set up sheet associations, if specified:
        if (jo.has("sheets")) {
            List<String> sheetNames = new LinkedList<String>();
            JSONArray jsn = jo.getJSONArray("sheets");
            for (int i = 0; i < jsn.length(); i++) {
                sheetNames.add(jsn.getString(i));
            }
            this.setSheetAssociations(curi, sheetNames);
        }

        // Set up recrawl interval, if specified:
        if (jo.has("recrawlInterval")) {
            int recrawlInterval = jo.getInt("recrawlInterval");
            curi.getData().put(RecentlySeenDecideRule.RECRAWL_INTERVAL,
                    recrawlInterval);
        }

        /*
         * Crawl job must be configured to use HighestUriQueuePrecedencePolicy
         * to ensure these high priority urls really get crawled ahead of
         * others. See
         * https://webarchive.jira.com/wiki/display/Heritrix/Precedence+
         * Feature+Notes
         */
        if (Hop.INFERRED.getHopString().equals(curi.getLastHop())) {
            curi.setSchedulingDirective(SchedulingConstants.HIGH);
            curi.setPrecedence(1);
        }

        curi.setForceFetch(jo.optBoolean("forceFetch"));
        curi.setSeed(jo.optBoolean("isSeed"));

        return curi;
    }

    /**
     * If the crawl request includes a list of Heritrix3 sheets to be associated
     * with the URL, this can be used to set the sheet associations up.
     * 
     */
    private void setSheetAssociations(CrawlURI curi, List<String> sheets) {
        // Get the SURT prefix to use:
        String prefix = curi.getUURI().getSurtForm(); // c.f.
        // org.archive.crawler.frontier.SurtAuthorityQueueAssignmentPolicy
        // Get the list of all known sheets:
        Set<String> allSheetNames = getSheetOverlaysManager().getSheetsByName()
                .keySet();
        // Make a list of the valid proposed sheet names:
        List<String> sheetNames = new LinkedList<String>();
        for (String sheetName : sheets) {
            if (allSheetNames.contains(sheetName)) {
                sheetNames.add(sheetName);
            } else {
                logger.severe("Unknown sheet name: " + sheetName);
            }
        }
        // Set the association for this prefix:
        logger.info("Setting sheets for " + prefix + " to " + sheetNames);
        getSheetOverlaysManager().getSheetsNamesBySurt().put(prefix,
                sheetNames);
    }

    // set the heritable data from the parent url, passed back to us via Kafka
    // XXX brittle, only goes one level deep, and only handles strings and
    // arrays, the latter of which it converts to a Set.
    // 'heritableData': {'source': 'https://facebook.com/whitehouse/',
    // 'heritable': ['source', 'heritable']}
    @SuppressWarnings("unchecked")
    protected void populateHeritableMetadata(CrawlURI curi,
            JSONObject parentUrlMetadata) {
        JSONObject heritableData = parentUrlMetadata
                .getJSONObject("heritableData");
        for (String key : (Set<String>) heritableData.keySet()) {
            Object value = heritableData.get(key);
            if (value instanceof JSONArray) {
                Set<String> valueSet = new HashSet<String>();
                JSONArray arr = ((JSONArray) value);
                for (int i = 0; i < arr.length(); i++) {
                    valueSet.add(arr.getString(i));
                }
                curi.getData().put(key, valueSet);
            } else {
                curi.getData().put(key, heritableData.get(key));
            }
        }
    }

    @Override
    public void onApplicationEvent(CrawlStateEvent event) {
        switch(event.getState()) {
        case PAUSING: case PAUSED:
            this.shutdown();
            break;

        case RUNNING:
            this.startup();
            break;

        default:
        }
    }

    @Override
    public void startCheckpoint(Checkpoint checkpointInProgress) {
        // No action needed.
    }

    @Override
    public void doCheckpoint(Checkpoint checkpointInProgress)
            throws IOException {
        // No action needed.
    }

    @Override
    public void finishCheckpoint(Checkpoint checkpointInProgress) {
        // No action needed.
    }

    @Override
    public void setRecoveryCheckpoint(Checkpoint recoveryCheckpoint) {
        // It seems we should recover from a checkpoint. In that case DO NOT
        // rewind the Kafka queue.
        this.seekToBeginning = false;

    }

    /* Reporter support */

    @Override
    public void reportTo(PrintWriter writer) throws IOException {
        writer.print("Kafka URL Receiver report - "
                + ArchiveUtils.get12DigitDate() + "\n");
        writer.println(" enqueued: " + this.enqueuedCount);
        writer.println(" discarded: " + this.discardedCount);
        writer.println("\n Partition Offsets:");
        List<Integer> keys = new ArrayList<Integer>(currentOffsets.keySet());
        Collections.sort(keys);
        for (Integer partition : keys) {
            writer.println(
                    "  partition: " + partition + ", offset: "
                            + currentOffsets.get(partition));
        }
        writer.println();
    }

    @Override
    public void shortReportLineTo(PrintWriter pw) throws IOException {
        pw.println("Kafka URL Receiver Report Line");

    }

    @Override
    public Map<String, Object> shortReportMap() {
        return null;
    }

    @Override
    public String shortReportLegend() {
        return "Kafka URL Receiver Report Legend";
    }

}
