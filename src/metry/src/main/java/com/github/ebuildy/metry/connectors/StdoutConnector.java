package com.github.ebuildy.metry.connectors;

import java.io.IOException;
import java.util.Properties;

public class StdoutConnector implements Connector {

    public StdoutConnector(final Properties properties) throws IOException {

    }

    /**
     * Send data as string via TCP.
     *
     * @param str string
     * @throws IOException
     */
    public void send(final String str) throws IOException {
       System.out.println(str);
    }
}
