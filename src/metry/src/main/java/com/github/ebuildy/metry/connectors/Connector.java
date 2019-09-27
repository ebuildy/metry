package com.github.ebuildy.metry.connectors;

import java.io.IOException;

public interface Connector {

    void send(final String str) throws IOException;

}
