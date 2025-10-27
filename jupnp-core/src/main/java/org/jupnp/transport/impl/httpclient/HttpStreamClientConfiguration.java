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
package org.jupnp.transport.impl.httpclient;

import java.util.concurrent.ExecutorService;

import org.jupnp.transport.spi.AbstractStreamClientConfiguration;

/**
 * Basic stream client configuration for the lightweight HTTP client implementation.
 */
public class HttpStreamClientConfiguration extends AbstractStreamClientConfiguration {

    public HttpStreamClientConfiguration(ExecutorService requestExecutorService) {
        super(requestExecutorService);
    }

    public HttpStreamClientConfiguration(ExecutorService requestExecutorService, int timeoutSeconds) {
        super(requestExecutorService, timeoutSeconds);
    }

    public HttpStreamClientConfiguration(ExecutorService requestExecutorService, int timeoutSeconds,
            int logWarningSeconds, int retryAfterSeconds, int retryIterations) {
        super(requestExecutorService, timeoutSeconds, logWarningSeconds, retryAfterSeconds, retryIterations);
    }
}
