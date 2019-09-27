package com.github.ebuildy.metry.reporters;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.github.ebuildy.metry.connectors.Connector;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.util.Utils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

@SuppressWarnings("rawtypes")


public class MetrySparkMetricsReporter extends ScheduledReporter {

    private final Logger LOGGER = LoggerFactory.getLogger(MetrySparkMetricsReporter.class);

    private final Clock clock;

    private final String timeStampString = "YYYY-MM-dd'T'HH:mm:ss.SSSZ";
    private SimpleDateFormat timestampFormat;

    private String appName = null;
    private String appId = null;
    private String executorId = null;

    /**
     * The output connector (TCP / HTTP ...).
     */
    private Connector connector;

    private Map<String, Object> rootMap;

    /**
     * Hold data map formatted keys.
     */
    private Map<String, String> sanitizeKeyCache = new HashMap<>();

    public MetrySparkMetricsReporter(MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit,
                                     TimeUnit durationUnit, Connector connector) {

        super(registry, "qwant-reporter", filter, rateUnit, durationUnit);

        this.clock = Clock.defaultClock();
        //this.timestampField = timestampField;
        this.timestampFormat = new SimpleDateFormat(timeStampString);

        this.connector = connector;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        if (this.connector == null) {
            return;
        }

        final long timestamp = clock.getTime();
        String timestampString = timestampFormat.format(new Date(timestamp));

        if (appName == null) {
            SparkEnv env = SparkEnv.get();
            SparkConf conf = env.conf();
            appName = conf.get("spark.app.name", "");
            appId = conf.getAppId();
            executorId = env.executorId();
        }

        try {
            final Map<String, Object> resultsMetrics = new HashMap<>();

            if (rootMap == null) {
                rootMap = new HashMap<>();

                rootMap.put("hostName", Utils.localHostName());
            }

            rootMap.put("application", new HashMap<String, String>() {{
                this.put("name", appName);
                this.put("id", appId);
            }});

            rootMap.put("executor", new HashMap<String, String>() {{
                this.put("id", executorId);
            }});

            rootMap.put("metrics", resultsMetrics);
            rootMap.put("date", timestampString);

            if (!gauges.isEmpty()) {
                for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                    reportGauge(resultsMetrics, entry);
                }
            }

            if (!counters.isEmpty()) {
                for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                    reportCounter(resultsMetrics, entry);
                }
            }

            if (!histograms.isEmpty()) {
                for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                    reportHistogram(resultsMetrics, entry);
                }
            }

            if (!meters.isEmpty()) {
                for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                    reportMeter(resultsMetrics, entry);
                }
            }

            if (!timers.isEmpty()) {
                for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                    reportTimer(resultsMetrics, entry);
                }
            }

            connector.send(new JSONObject(rootMap).toString());

        } catch (IOException ioe) {
            //LOGGER.error("Exception posting to Elasticsearch index: " + ioe.toString());
            ioe.printStackTrace();
        }
    }

    /**
     * Fill output data.
     *
     * @param rootMap
     * @param metricName
     * @param metricValue
     */
    private void bindDataMap(final Map<String, Object> rootMap, final String metricName, Object metricValue) {
        final String metric = getRealMetricName(metricName);
        final String[] paths = metric.split("\\.");
        int i = 0;
        Map<String, Object> resultsMapMetric = rootMap;

        if (metricValue instanceof Double) {
            metricValue = ((Double) metricValue).floatValue();
        }

        for (String path : paths) {
            path = sanitizeKey(path);

            if (i == paths.length - 1) {
                resultsMapMetric.put(path, metricValue);
            } else {
                if (!resultsMapMetric.containsKey(path)) {
                    resultsMapMetric.put(path, new HashMap<String, Object>());
                }

                resultsMapMetric = (Map<String, Object>) resultsMapMetric.get(path);
            }

            i++;
        }
    }

    private void reportGauge(Map<String, Object> resultsMap, Map.Entry<String, Gauge> entry) {
        bindDataMap(resultsMap, entry.getKey(), entry.getValue().getValue());
    }

    private void reportCounter(Map<String, Object> resultsMap, Entry<String, Counter> entry) {
        bindDataMap(resultsMap, entry.getKey(), entry.getValue().getCount());
    }

    private void reportHistogram(Map<String, Object> resultsMap, Entry<String, Histogram> entry) {
        final Map<String, Object> value = new HashMap<>();
        final Histogram histogram = entry.getValue();
        final Snapshot snapshot = histogram.getSnapshot();

        value.put("count", histogram.getCount());
        value.put("min", convertDuration(snapshot.getMin()));
        value.put("max", convertDuration(snapshot.getMax()));
        value.put("mean", convertDuration(snapshot.getMean()));
        value.put("stddev", convertDuration(snapshot.getStdDev()));
        value.put("median", convertDuration(snapshot.getMedian()));
        value.put("percentile", new HashMap<String, Double>() {{
            this.put("75th", convertDuration(snapshot.get75thPercentile()));
            this.put("95th", convertDuration(snapshot.get95thPercentile()));
            this.put("98th", convertDuration(snapshot.get98thPercentile()));
            this.put("99th", convertDuration(snapshot.get99thPercentile()));
            this.put("999th", convertDuration(snapshot.get999thPercentile()));
        }});

        bindDataMap(resultsMap, entry.getKey(), value);
    }

    private void reportMeter(Map<String, Object> resultsMap, Entry<String, Meter> entry) {
        final Map<String, Object> value = new HashMap<>();
        final Meter meter = entry.getValue();

        value.put("count", meter.getCount());
        value.put("rate", new HashMap<String, Double>() {{
            this.put("mean", convertRate(meter.getMeanRate()));
            this.put("1m", convertRate(meter.getOneMinuteRate()));
            this.put("5m", convertRate(meter.getFiveMinuteRate()));
            this.put("15m", convertRate(meter.getFifteenMinuteRate()));
        }});

        bindDataMap(resultsMap, entry.getKey(), value);
    }

    private void reportTimer(Map<String, Object> resultsMap, Entry<String, Timer> entry) {
        final Map<String, Object> value = new HashMap<>();
        final Timer timer = entry.getValue();
        final Snapshot snapshot = timer.getSnapshot();

        value.put("count", timer.getCount());
        value.put("mean", convertDuration(snapshot.getMean()));
        value.put("min", convertDuration(snapshot.getMin()));
        value.put("max", convertDuration(snapshot.getMax()));
        value.put("stddev", convertDuration(snapshot.getStdDev()));
        value.put("median", convertDuration(snapshot.getMedian()));

        value.put("rate", new HashMap<String, Double>() {{
            this.put("mean", convertRate(timer.getMeanRate()));
            this.put("1m", convertRate(timer.getOneMinuteRate()));
            this.put("5m", convertRate(timer.getFiveMinuteRate()));
            this.put("15m", convertRate(timer.getFifteenMinuteRate()));
        }});

        value.put("percentile", new HashMap<String, Double>() {{
            this.put("75th", convertDuration(snapshot.get75thPercentile()));
            this.put("95th", convertDuration(snapshot.get95thPercentile()));
            this.put("98th", convertDuration(snapshot.get98thPercentile()));
            this.put("99th", convertDuration(snapshot.get99thPercentile()));
            this.put("999th", convertDuration(snapshot.get999thPercentile()));
        }});

        bindDataMap(resultsMap, entry.getKey(), value);
    }

    private String getRealMetricName(String name) {

        // Parse name to extract application id etc
        // The appid and executorId should be the same for all metrics so we can
        // process just the first one
        if (appId == null || executorId == null) {
            String nameParts[] = name.split("\\.");
            appId = nameParts[0];
            executorId = nameParts[1];
        }

        final String metricName = (name.substring(appId.length() + executorId.length() + 2));//.replace('.', '_');

        return metricName;
    }

    private String sanitizeKey(final String k) {

        if (!sanitizeKeyCache.containsKey(k)) {
            sanitizeKeyCache.put(k, snakeCaseFormat(k).replace("-", "_").replace("\\.", "_"));
        }

        return sanitizeKeyCache.get(k);
    }

    /**
     * Convert to UPPER_UNDERSCORE format detecting upper case acronyms
     */
    private static String snakeCaseFormat(String name) {
        final StringBuilder result = new StringBuilder();

        boolean lastUppercase = false;

        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            char lastEntry = i == 0 ? 'X' : result.charAt(result.length() - 1);
            if (ch == ' ' || ch == '_' || ch == '-' || ch == '.') {
                lastUppercase = false;

                if (lastEntry == '_') {
                    continue;
                } else {
                    ch = '_';
                }
            } else if (Character.isUpperCase(ch)) {
                ch = Character.toLowerCase(ch);
                // is start?
                if (i > 0) {
                    if (lastUppercase) {
                        // test if end of acronym
                        if (i + 1 < name.length()) {
                            char next = name.charAt(i + 1);
                            if (!Character.isUpperCase(next) && Character.isAlphabetic(next)) {
                                // end of acronym
                                if (lastEntry != '_') {
                                    result.append('_');
                                }
                            }
                        }
                    } else {
                        // last was lowercase, insert _
                        if (lastEntry != '_') {
                            result.append('_');
                        }
                    }
                }
                lastUppercase = true;
            } else {
                lastUppercase = false;
            }

            result.append(ch);
        }
        return result.toString();
    }
}
