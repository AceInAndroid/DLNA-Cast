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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.lang.reflect.Field;

import org.jupnp.model.message.StreamRequestMessage;
import org.jupnp.model.message.StreamResponseMessage;
import org.jupnp.model.message.UpnpHeaders;
import org.jupnp.model.message.UpnpMessage;
import org.jupnp.model.message.UpnpRequest;
import org.jupnp.model.message.UpnpResponse;
import org.jupnp.model.message.header.UpnpHeader;
import org.jupnp.transport.spi.AbstractStreamClient;
import org.jupnp.transport.spi.InitializationException;
import org.jupnp.util.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight {@link org.jupnp.transport.spi.StreamClient} backed by {@link HttpURLConnection}.
 */
public class HttpStreamClientImpl
        extends AbstractStreamClient<HttpStreamClientConfiguration, HttpStreamClientImpl.RequestWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpStreamClientImpl.class);

    private static final String DEFAULT_CONTENT_TYPE = "text/xml; charset=\"utf-8\"";

    private final HttpStreamClientConfiguration configuration;

    public HttpStreamClientImpl(HttpStreamClientConfiguration configuration) throws InitializationException {
        this.configuration = configuration;
    }

    @Override
    public HttpStreamClientConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    protected RequestWrapper createRequest(StreamRequestMessage requestMessage) {
        try {
            // Ensure Host header exists
            ensureHostHeader(requestMessage);

            // Default User-Agent if missing
            ensureUserAgentHeader(requestMessage);

            UpnpRequest.Method method = requestMessage.getOperation().getMethod();
            ensureContentTypeHeader(requestMessage, method);
            ensureConnectionHeader(requestMessage);

            // Apply headers
            UpnpHeaders headers = requestMessage.getHeaders();
            LOGGER.trace("Prepared HTTP request with {} headers for {}", headers.size(), requestMessage.getUri());
            return new RequestWrapper(requestMessage.getUri());
        } catch (Exception ex) {
            LOGGER.warn("Failed to create HTTP request for {}", requestMessage.getUri(), ex);
            return null;
        }
    }

    private void ensureHostHeader(StreamRequestMessage requestMessage) {
        if (!requestMessage.getHeaders().containsKey("Host")) {
            URI uri = requestMessage.getUri();
            int port = uri.getPort();
            String host = uri.getHost();
            if (host != null && host.contains(":" ) && !host.startsWith("[")) {
                host = "[" + host + "]";
            }
            String hostHeader = port > 0 ? host + ":" + port : host;
            requestMessage.getHeaders().add("Host", hostHeader);
        }
    }

    private void ensureUserAgentHeader(StreamRequestMessage requestMessage) {
        if (!requestMessage.getHeaders().containsKey(UpnpHeader.Type.USER_AGENT.getHttpName())) {
            String userAgent = configuration.getUserAgentValue(requestMessage.getUdaMajorVersion(),
                    requestMessage.getUdaMinorVersion());
            requestMessage.getHeaders().add(UpnpHeader.Type.USER_AGENT.getHttpName(), userAgent);
        }
    }

    private void ensureConnectionHeader(StreamRequestMessage requestMessage) {
        if (!requestMessage.getHeaders().containsKey("Connection")) {
            requestMessage.getHeaders().add("Connection", "close");
        }
        if (!requestMessage.getHeaders().containsKey("Accept")) {
            requestMessage.getHeaders().add("Accept", "*/*");
        }
    }

    private void ensureContentTypeHeader(StreamRequestMessage requestMessage, UpnpRequest.Method method) {
        if (!supportsRequestBody(method)) {
            return;
        }
        if (requestMessage.getHeaders().containsKey("Content-Type")) {
            return;
        }
        if (requestMessage.hasBody()) {
            requestMessage.getHeaders().add("Content-Type", DEFAULT_CONTENT_TYPE);
        }
    }

    private boolean supportsRequestBody(UpnpRequest.Method method) {
        if (method == null) {
            return false;
        }
        return method == UpnpRequest.Method.POST || method == UpnpRequest.Method.NOTIFY;
    }

    @Override
    protected Callable<StreamResponseMessage> createCallable(StreamRequestMessage requestMessage,
            RequestWrapper request) {
        return () -> executeCall(requestMessage, request);
    }

    private StreamResponseMessage executeCall(StreamRequestMessage requestMessage, RequestWrapper request)
            throws IOException {
        HttpURLConnection connection = null;
        try {
            URL url = requestMessage.getUri().toURL();
            connection = (HttpURLConnection) url.openConnection();
            request.connection = connection;

            int timeoutMillis = (int) TimeUnit.SECONDS.toMillis(configuration.getTimeoutSeconds());
            connection.setConnectTimeout(timeoutMillis);
            connection.setReadTimeout(timeoutMillis);
            connection.setInstanceFollowRedirects(true);
            connection.setUseCaches(false);
            connection.setDoInput(true);

            UpnpRequest.Method method = requestMessage.getOperation().getMethod();
            setRequestMethod(connection, method.getHttpName());

            applyHeaders(connection, requestMessage.getHeaders());

            boolean hasBody = supportsRequestBody(method) && requestMessage.hasBody();
            if (hasBody) {
                byte[] body = requestMessage.getBodyBytes();
                connection.setDoOutput(true);
                connection.setFixedLengthStreamingMode(body.length);
                try (OutputStream outputStream = connection.getOutputStream()) {
                    outputStream.write(body);
                    outputStream.flush();
                }
            } else {
                connection.setDoOutput(false);
            }

            if (!hasBody) {
                connection.connect();
            }

            int status = connection.getResponseCode();
            String message = connection.getResponseMessage();
            if (message == null) {
                message = "";
            }
            LOGGER.trace("Received HTTP response: {} {}", status, message);

            UpnpResponse upnpResponse = new UpnpResponse(status, message);
            StreamResponseMessage responseMessage = new StreamResponseMessage(upnpResponse);
            responseMessage.setHeaders(convertHeaders(connection));

            byte[] bytes = readResponseBytes(connection);
            if (bytes.length > 0) {
                if (responseMessage.isContentTypeMissingOrText()) {
                    responseMessage.setBodyCharacters(bytes);
                } else {
                    responseMessage.setBody(UpnpMessage.BodyType.BYTES, bytes);
                }
            }
            return responseMessage;
        } catch (IOException ex) {
            LOGGER.warn("HTTP request failed: {}", requestMessage.getUri(), Exceptions.unwrap(ex));
            throw ex;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
            request.connection = null;
        }
    }

    private void setRequestMethod(HttpURLConnection connection, String method) throws IOException {
        try {
            connection.setRequestMethod(method);
        } catch (ProtocolException ex) {
            if (!applyMethodViaReflection(connection, method)) {
                throw ex;
            }
        }
    }

    private boolean applyMethodViaReflection(HttpURLConnection connection, String method) {
        try {
            boolean updated = setField(connection, "method", method);
            updated |= updateDelegateMethod(connection, method);
            return updated;
        } catch (Exception ex) {
            LOGGER.debug("Failed to set HTTP method via reflection: {}", method, ex);
            return false;
        }
    }

    private boolean updateDelegateMethod(HttpURLConnection connection, String method) throws IllegalAccessException {
        try {
            Field delegateField = findField(connection.getClass(), "delegate");
            if (delegateField != null) {
                delegateField.setAccessible(true);
                Object delegate = delegateField.get(connection);
                if (delegate instanceof HttpURLConnection) {
                    return applyMethodViaReflection((HttpURLConnection) delegate, method);
                }
            }
        } catch (Exception ex) {
            LOGGER.trace("Failed updating delegate method via reflection", ex);
        }
        return false;
    }

    private boolean setField(Object target, String fieldName, Object value)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = findField(target.getClass(), fieldName);
        if (field == null) {
            return false;
        }
        field.setAccessible(true);
        field.set(target, value);
        return true;
    }

    private Field findField(Class<?> type, String fieldName) {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ex) {
                current = current.getSuperclass();
            }
        }
        return null;
    }

    private void applyHeaders(HttpURLConnection connection, UpnpHeaders headers) {
        for (String headerName : headers.keySet()) {
            List<String> values = headers.get(headerName);
            if (values == null || values.isEmpty()) {
                continue;
            }
            boolean first = true;
            for (String value : values) {
                if (first) {
                    connection.setRequestProperty(headerName, value);
                    first = false;
                } else {
                    connection.addRequestProperty(headerName, value);
                }
            }
        }
    }

    private UpnpHeaders convertHeaders(HttpURLConnection connection) {
        UpnpHeaders upnpHeaders = new UpnpHeaders();
        Map<String, List<String>> headerFields = connection.getHeaderFields();
        if (headerFields == null) {
            return upnpHeaders;
        }
        for (Map.Entry<String, List<String>> entry : headerFields.entrySet()) {
            String name = entry.getKey();
            if (name == null) {
                continue;
            }
            List<String> values = entry.getValue();
            if (values == null) {
                continue;
            }
            for (String value : values) {
                upnpHeaders.add(name, value);
            }
        }
        return upnpHeaders;
    }

    private byte[] readResponseBytes(HttpURLConnection connection) throws IOException {
        InputStream stream = null;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            int status = connection.getResponseCode();
            if (status >= HttpURLConnection.HTTP_BAD_REQUEST) {
                stream = connection.getErrorStream();
            }
            if (stream == null) {
                stream = connection.getInputStream();
            }
            if (stream == null) {
                return new byte[0];
            }
            byte[] data = new byte[4096];
            int read;
            while ((read = stream.read(data)) != -1) {
                buffer.write(data, 0, read);
            }
            return buffer.toByteArray();
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException ex) {
                    LOGGER.debug("Error closing HTTP response stream", ex);
                }
            }
        }
    }

    @Override
    protected void abort(RequestWrapper request) {
        HttpURLConnection connection = request.connection;
        if (connection != null) {
            connection.disconnect();
        }
    }

    @Override
    protected boolean logExecutionException(Throwable t) {
        return false;
    }

    @Override
    protected void onFinally(RequestWrapper request) {
        // Nothing to do
    }

    @Override
    public void stop() {
        // Nothing to stop for HttpURLConnection
    }

    static class RequestWrapper {
        final URI uri;
        volatile HttpURLConnection connection;

        RequestWrapper(URI uri) {
            this.uri = uri;
        }
    }
}
