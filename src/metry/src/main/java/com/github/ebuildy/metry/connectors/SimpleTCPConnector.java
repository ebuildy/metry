package com.github.ebuildy.metry.connectors;

import com.github.ebuildy.metry.MetrySink;
import org.mortbay.util.UrlEncoded;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.*;
import java.util.Properties;

public class SimpleTCPConnector implements Connector {

    private Socket socket;

    private BufferedWriter outToServer;

    private static int defaultCharBufferSize = 2*8192;

    private static int flushCounterTrigger = 5;

    private int flushCounter = 0;

    private final String host;
    private final int port;

    public SimpleTCPConnector(final Properties properties) throws IOException {
        final URI url;

        try {
            url = new URI(properties.getProperty(MetrySink.KEY_URL));
        } catch (URISyntaxException e) {
            throw new IOException(e.getMessage(), e);
        }

        this.host = url.getHost();
        this.port = url.getPort();
    }

    private BufferedWriter getOutputStream() throws IOException {
        if (socket == null) {
            socket = new Socket(host, port);
        }

        if (outToServer == null) {
            outToServer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()), defaultCharBufferSize);
        }

        return outToServer;
    }

    /**
     * Send data as string via TCP.
     *
     * @param str string
     * @throws IOException
     */
    public void send(final String str) throws IOException {
        final BufferedWriter writer = getOutputStream();

        writer.write(str.concat("\n"));
        writer.flush();

        if (this.flushCounter++ == flushCounterTrigger) {
            this.flushCounter = 0;
            this.outToServer.flush();
        }
    }
}
