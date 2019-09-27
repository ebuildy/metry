package com.github.ebuildy.metry.connectors;

import com.github.ebuildy.metry.MetrySink;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;


public class HttpConnectorTest {

    static final private int HTTP_PORT = 30020;

    private MockWebServer server;

    @Test public void testSingleHttpConnector() throws Exception {
        server = new MockWebServer();

        server.enqueue(new MockResponse().setBody("ok"));

        server.start(HTTP_PORT);

        Properties p = new Properties();

        p.putIfAbsent(MetrySink.KEY_URL, "http://localhost:" + HTTP_PORT + "/PATH");
        p.putIfAbsent(MetrySink.KEY_METRY_VERSION, "1.0");
        p.putIfAbsent(MetrySink.KEY_JAVA_VERSION, Runtime.class.getPackage().getImplementationVersion());
        p.putIfAbsent(MetrySink.KEY_SPARK_VERSION, "2.4");

        HTTPConnector classUnderTest = new HTTPConnector(p);

        classUnderTest.send("hello world");

        RecordedRequest request = server.takeRequest();

        Assert.assertEquals("POST /PATH HTTP/1.1", request.getRequestLine());
        Assert.assertEquals("application/json; charset=utf-8", request.getHeader("Content-Type"));
        Assert.assertEquals("hello world", request.getBody().readUtf8());

        server.shutdown();
    }


    @Test
    public void runSpark() throws Exception {
        server = new MockWebServer();

        server.enqueue(new MockResponse().setBody("ok"));

        server.start(HTTP_PORT);

        final SparkConf esSparkConf = new SparkConf();

        esSparkConf.setMaster("local");
        esSparkConf.setAppName("metry_test");
        esSparkConf.setIfMissing("spark.metrics.conf", "./src/test/resources/metrics.properties");

        final JavaSparkContext sc = new JavaSparkContext(esSparkConf);

        sc.emptyRDD().cache();

        sc.parallelize(Arrays.asList("a", "b", "c", "d", "e")).cache();

        sc.parallelize(Arrays.asList("a", "b", "c", "d", "e")).count();

        RecordedRequest request = server.takeRequest();

        Assert.assertEquals("POST / HTTP/1.1", request.getRequestLine());
        Assert.assertEquals("application/json; charset=utf-8", request.getHeader("Content-Type"));
        Assert.assertTrue(request.getBody().readUtf8().startsWith("{"));

        server.shutdown();
    }

}
