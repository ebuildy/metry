package com.github.ebuildy.metry;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.github.ebuildy.metry.connectors.Connector;
import com.github.ebuildy.metry.connectors.HTTPConnector;
import com.github.ebuildy.metry.connectors.SimpleTCPConnector;
import com.github.ebuildy.metry.reporters.MetrySparkMetricsReporter;
import org.apache.spark.SecurityManager;
import org.apache.spark.metrics.sink.Sink;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MetrySink implements Sink {

    final public static String KEY_INDEX = "index";
    final public static String KEY_PERIOD = "period";
    final public static String KEY_UNIT = "unit";

    final public static String KEY_URL = "url";
    final public static String KEY_METRY_VERSION = "metry.version";
    final public static String KEY_SPARK_VERSION = "spark.version";
    final public static String KEY_JAVA_VERSION = "java.version";

    // Defaults
    final Integer DEFAULT_PERIOD = 10;
    final String DEFAULT_UNIT = "SECONDS";
    final String DEFAULT_INDEX = "spark";

    private MetrySparkMetricsReporter reporter;

    private TimeUnit periodUnit = TimeUnit.SECONDS;
    private long periodValue = 5;

    public MetrySink(final Properties properties, final MetricRegistry registry, final SecurityManager securityManager) {
        Connector connector = null;

        try {
            connector = getConnector(properties);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (connector != null) {
            this.reporter = new MetrySparkMetricsReporter(registry, MetricFilter.ALL, periodUnit, periodUnit, connector);
        }
    }

    static public Connector getConnector(final Properties properties) throws IOException {
        properties.putIfAbsent(KEY_METRY_VERSION, "1.0");
        properties.putIfAbsent(KEY_JAVA_VERSION, Runtime.class.getPackage().getImplementationVersion());
        properties.putIfAbsent(KEY_SPARK_VERSION, "2.4"); // @todo get real Spark version

        final String url = properties.getProperty(MetrySink.KEY_URL);

        if (url.startsWith("http:") || url.startsWith("https:")) {
            return new HTTPConnector(properties);
        }

        return new SimpleTCPConnector(properties);
    }

    @Override
    public void start() {
        if (reporter != null) {
            reporter.start(periodValue, periodUnit);
        }
    }

    @Override
    public void stop() {
        if (reporter != null) {
            reporter.stop();
        }
    }

    @Override
    public void report() {
        if (reporter != null) {
            reporter.report();
        }
    }
}
