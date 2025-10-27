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
package org.jupnp.transport.impl.httpserver;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jupnp.transport.Router;
import org.jupnp.transport.impl.httpserver.internal.NanoHttpUpnpStream;
import org.jupnp.transport.spi.InitializationException;
import org.jupnp.transport.spi.StreamServer;
import org.jupnp.util.io.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fi.iki.elonen.NanoHTTPD;

/**
 * Lightweight {@link StreamServer} implementation backed by {@link NanoHTTPD}.
 */
public class HttpStreamServerImpl implements StreamServer<HttpStreamServerConfiguration> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpStreamServerImpl.class);
    private static final long RESPONSE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(15);

    private final HttpStreamServerConfiguration configuration;

    private Router router;
    private InetAddress bindAddress;
    private NanoHttpServerAdapter httpServer;
    private String contextPath;
    private int listeningPort;

    public HttpStreamServerImpl(HttpStreamServerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public HttpStreamServerConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public synchronized void init(InetAddress bindAddress, Router router) throws InitializationException {
        this.router = router;
        this.bindAddress = bindAddress;

        contextPath = router.getConfiguration().getNamespace().getBasePath().getPath();
        if (contextPath == null || contextPath.isEmpty()) {
            contextPath = "/";
        }

        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(bindAddress, configuration.getListenPort()));
            listeningPort = socket.getLocalPort();
        } catch (IOException ex) {
            throw new InitializationException("Could not bind HTTP stream server socket", ex);
        }

        httpServer = new NanoHttpServerAdapter(listeningPort);
        LOGGER.debug("Initialized HTTP stream server on {}:{}", bindAddress, listeningPort);
    }

    @Override
    public synchronized int getPort() {
        return listeningPort;
    }

    @Override
    public synchronized void stop() {
        if (httpServer != null) {
            LOGGER.debug("Stopping HTTP stream server");
            httpServer.stop();
        }
    }

    @Override
    public synchronized void run() {
        if (httpServer == null) {
            LOGGER.warn("HTTP stream server not initialized, cannot start");
            return;
        }
        try {
            LOGGER.debug("Starting HTTP stream server on {}:{}", bindAddress, listeningPort);
            httpServer.start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
        } catch (IOException ex) {
            LOGGER.error("Failed to start HTTP stream server", ex);
        }
    }

    private final class NanoHttpServerAdapter extends NanoHTTPD {

        private final String normalizedContextPath;

        private NanoHttpServerAdapter(int port) {
            super(bindAddress.getHostAddress(), port);
            normalizedContextPath = normalizeContextPath(contextPath);
        }

        @Override
        public Response serve(IHTTPSession session) {
            if (!isPathInContext(session.getUri())) {
                return NanoHTTPD.newFixedLengthResponse(Response.Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "");
            }

            byte[] body;
            try {
                body = readRequestBody(session);
            } catch (IOException ex) {
                LOGGER.warn("Failed reading HTTP request body for {} {}", session.getMethod(), session.getUri(), ex);
                return NanoHTTPD.newFixedLengthResponse(Response.Status.INTERNAL_ERROR, NanoHTTPD.MIME_PLAINTEXT, "");
            }

            CompletableFuture<Response> responseFuture = new CompletableFuture<>();
            NanoHttpUpnpStream stream = new NanoHttpUpnpStream(router.getProtocolFactory(), session, body, bindAddress,
                    responseFuture);
            router.received(stream);

            try {
                return responseFuture.get(RESPONSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted while waiting for UPnP response", ex);
                return NanoHTTPD.newFixedLengthResponse(Response.Status.INTERNAL_ERROR, NanoHTTPD.MIME_PLAINTEXT, "");
            } catch (ExecutionException ex) {
                LOGGER.warn("Execution error while processing HTTP request", ex.getCause());
                return NanoHTTPD.newFixedLengthResponse(Response.Status.INTERNAL_ERROR, NanoHTTPD.MIME_PLAINTEXT, "");
            } catch (TimeoutException ex) {
                LOGGER.warn("Timeout processing HTTP request {} {}", session.getMethod(), session.getUri());
                return NanoHTTPD.newFixedLengthResponse(createStatus(504, "Gateway Timeout"),
                        NanoHTTPD.MIME_PLAINTEXT, "");
            }
        }

        private boolean isPathInContext(String uriPath) {
            if ("/".equals(normalizedContextPath)) {
                return true;
            }
            if (uriPath == null) {
                return false;
            }
            if (uriPath.equals(normalizedContextPath)) {
                return true;
            }
            return uriPath.startsWith(normalizedContextPath.endsWith("/") ? normalizedContextPath
                    : normalizedContextPath + "/");
        }

        private String normalizeContextPath(String path) {
            if (path == null || path.isEmpty()) {
                return "/";
            }
            if (!path.startsWith("/")) {
                return "/" + path;
            }
            return path;
        }

        private byte[] readRequestBody(IHTTPSession session) throws IOException {
            InputStream inputStream = session.getInputStream();
            if (inputStream == null) {
                return new byte[0];
            }

            int contentLength = parseContentLength(session.getHeaders());
            if (contentLength > 0) {
                return readFixedLength(inputStream, contentLength);
            }

            return IO.readAllBytes(inputStream);
        }

        private int parseContentLength(Map<String, String> headers) {
            if (headers == null || headers.isEmpty()) {
                return 0;
            }
            String value = headers.get("content-length");
            if (value == null) {
                value = headers.get("Content-Length");
            }
            if (value == null) {
                return 0;
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException ex) {
                LOGGER.trace("Invalid content-length header value: {}", value);
                return 0;
            }
        }

        private byte[] readFixedLength(InputStream inputStream, int length) throws IOException {
            byte[] buffer = new byte[length];
            int offset = 0;
            while (offset < length) {
                int read = inputStream.read(buffer, offset, length - offset);
                if (read == -1) {
                    break;
                }
                offset += read;
            }
            if (offset == length) {
                return buffer;
            }
            byte[] result = new byte[offset];
            System.arraycopy(buffer, 0, result, 0, offset);
            return result;
        }
    }

    private NanoHTTPD.Response.IStatus createStatus(final int statusCode, final String description) {
        final String resolvedDescription = description != null ? description : "";
        return new NanoHTTPD.Response.IStatus() {
            @Override
            public int getRequestStatus() {
                return statusCode;
            }

            @Override
            public String getDescription() {
                return resolvedDescription;
            }
        };
    }
}
