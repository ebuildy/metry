package com.github.ebuildy.metry.connectors;

import com.github.ebuildy.metry.MetrySink;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class HTTPConnector implements Connector {

    final private HttpURLConnection httpURLConnection;

    private OutputStream outputStream = null;

    public HTTPConnector(final Properties properties) throws IOException {
        final URL url = new URL(properties.getProperty(MetrySink.KEY_URL));

        final String userAgent = String.format("Metry/%s Spark/%s Java/%s",
            properties.getProperty(MetrySink.KEY_METRY_VERSION),
            properties.getProperty(MetrySink.KEY_SPARK_VERSION),
            properties.getProperty(MetrySink.KEY_JAVA_VERSION)
        );

        httpURLConnection = (HttpURLConnection) url.openConnection();

        httpURLConnection.setRequestProperty("Content-Type", "application/json; charset=utf-8");
        httpURLConnection.setRequestProperty("User-Agent", userAgent);
        httpURLConnection.setRequestMethod("POST");
        httpURLConnection.setDoOutput(true);
        httpURLConnection.setDoInput(true);
        //httpURLConnection.setUseCaches(false);
        httpURLConnection.setConnectTimeout(1000);

        //httpURLConnection.connect();
    }

    /**
     * Send data as string via TCP.
     *
     * @param str string
     * @throws IOException
     */
    public void send(final String str) throws IOException {
        if (outputStream == null) {
            outputStream = httpURLConnection.getOutputStream();
        }

        outputStream.write(str.getBytes());
        outputStream.flush();

        httpURLConnection.getResponseCode();
    }
}
