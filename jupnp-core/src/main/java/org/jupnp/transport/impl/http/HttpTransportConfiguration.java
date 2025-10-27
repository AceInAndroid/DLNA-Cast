/*
 * Copyright (C) 2011-2025 4th Line GmbH, Switzerland and others
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License Version 1 or later
 * ("CDDL") (collectively, the "License"). You may not use this file
 * except in compliance with the License. See LICENSE.txt for more
 * information.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * SPDX-License-Identifier: CDDL-1.0
 */
package org.jupnp.transport.impl.http;

import java.util.concurrent.ExecutorService;

import org.jupnp.transport.TransportConfiguration;
import org.jupnp.transport.impl.httpclient.HttpStreamClientConfiguration;
import org.jupnp.transport.impl.httpclient.HttpStreamClientImpl;
import org.jupnp.transport.impl.httpserver.HttpStreamServerConfiguration;
import org.jupnp.transport.impl.httpserver.HttpStreamServerImpl;
import org.jupnp.transport.spi.InitializationException;
import org.jupnp.transport.spi.StreamClient;
import org.jupnp.transport.spi.StreamClientConfiguration;
import org.jupnp.transport.spi.StreamServer;
import org.jupnp.transport.spi.StreamServerConfiguration;

/**
 * Lightweight transport configuration using OkHttp for the client and NanoHTTPD for the server.
 */
public class HttpTransportConfiguration
        implements TransportConfiguration<HttpStreamClientConfiguration, HttpStreamServerConfiguration> {

    @Override
    public StreamClient<HttpStreamClientConfiguration> createStreamClient(ExecutorService executorService,
            StreamClientConfiguration configuration) {
        HttpStreamClientConfiguration clientConfiguration = configuration instanceof HttpStreamClientConfiguration
                ? (HttpStreamClientConfiguration) configuration
                : new HttpStreamClientConfiguration(executorService);
        try {
            return new HttpStreamClientImpl(clientConfiguration);
        } catch (InitializationException e) {
            throw new RuntimeException("Failed to create HTTP stream client", e);
        }
    }

    @Override
    public StreamServer<HttpStreamServerConfiguration> createStreamServer(int listenerPort) {
        return new HttpStreamServerImpl(new HttpStreamServerConfiguration(listenerPort));
    }
}
