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
package org.jupnp.transport.impl.httpserver.internal;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.jupnp.model.message.Connection;
import org.jupnp.model.message.StreamRequestMessage;
import org.jupnp.model.message.StreamResponseMessage;
import org.jupnp.model.message.UpnpHeaders;
import org.jupnp.model.message.UpnpMessage;
import org.jupnp.model.message.UpnpRequest;
import org.jupnp.protocol.ProtocolFactory;
import org.jupnp.transport.spi.UpnpStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fi.iki.elonen.NanoHTTPD;

/**
 * Adapter that bridges {@link NanoHTTPD} requests into the UPnP stack.
 */
public class NanoHttpUpnpStream extends UpnpStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(NanoHttpUpnpStream.class);

    private final NanoHTTPD.IHTTPSession session;
    private final byte[] requestBody;
    private final InetAddress localAddress;
    private final CompletableFuture<NanoHTTPD.Response> responseFuture;

    public NanoHttpUpnpStream(ProtocolFactory protocolFactory, NanoHTTPD.IHTTPSession session, byte[] requestBody,
            InetAddress localAddress, CompletableFuture<NanoHTTPD.Response> responseFuture) {
        super(protocolFactory);
        this.session = session;
        this.requestBody = requestBody != null ? requestBody : new byte[0];
        this.localAddress = localAddress;
        this.responseFuture = responseFuture;
    }

    @Override
    public void run() {
        StreamResponseMessage responseMessage = null;
        try {
            StreamRequestMessage requestMessage = buildRequestMessage();
            responseMessage = process(requestMessage);
            NanoHTTPD.Response response = buildResponse(responseMessage);
            if (!responseFuture.complete(response)) {
                LOGGER.trace("HTTP response future already completed for {} {}", session.getMethod(), session.getUri());
            }
            responseSent(responseMessage);
        } catch (Exception ex) {
            LOGGER.warn("Error processing HTTP request {} {}", session.getMethod(), session.getUri(), ex);
            NanoHTTPD.Response errorResponse = NanoHTTPD.newFixedLengthResponse(
                    NanoHTTPD.Response.Status.INTERNAL_ERROR, NanoHTTPD.MIME_PLAINTEXT, "");
            responseFuture.complete(errorResponse);
            responseException(ex);
        }
    }

    private StreamRequestMessage buildRequestMessage() {
        String rawUri = session.getUri();
        if (rawUri == null || rawUri.isEmpty()) {
            rawUri = "/";
        }
        String query = session.getQueryParameterString();
        String requestUriString = query == null || query.isEmpty() ? rawUri : rawUri + "?" + query;
        URI requestUri = URI.create(requestUriString);

        UpnpRequest.Method method = UpnpRequest.Method.getByHttpName(session.getMethod().name());
        if (method == UpnpRequest.Method.UNKNOWN) {
            throw new IllegalArgumentException("Unsupported HTTP method: " + session.getMethod());
        }

        StreamRequestMessage requestMessage = new StreamRequestMessage(method, requestUri);
        requestMessage.getOperation().setHttpMinorVersion(1);
        requestMessage.setConnection(createConnection());

        UpnpHeaders headers = convertHeaders(session.getHeaders());
        requestMessage.setHeaders(headers);

        if (requestBody.length > 0) {
            if (requestMessage.isContentTypeMissingOrText()) {
                requestMessage.setBodyCharacters(requestBody);
            } else {
                requestMessage.setBody(UpnpMessage.BodyType.BYTES, requestBody);
            }
        }

        return requestMessage;
    }

    private Connection createConnection() {
        return new Connection() {
            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public InetAddress getRemoteAddress() {
                String remoteIp = session.getRemoteIpAddress();
                if (remoteIp == null || remoteIp.isEmpty()) {
                    return null;
                }
                try {
                    return InetAddress.getByName(remoteIp);
                } catch (UnknownHostException ex) {
                    LOGGER.trace("Unable to resolve remote address: {}", remoteIp, ex);
                    return null;
                }
            }

            @Override
            public InetAddress getLocalAddress() {
                return localAddress;
            }
        };
    }

    private UpnpHeaders convertHeaders(Map<String, String> headers) {
        UpnpHeaders upnpHeaders = new UpnpHeaders();
        if (headers == null) {
            return upnpHeaders;
        }
        headers.forEach((key, value) -> {
            if (key != null && value != null) {
                upnpHeaders.add(key, value);
            }
        });
        return upnpHeaders;
    }

    private NanoHTTPD.Response buildResponse(StreamResponseMessage responseMessage) {
        if (responseMessage == null) {
            return NanoHTTPD.newFixedLengthResponse(createStatus(404, "Not Found"), NanoHTTPD.MIME_PLAINTEXT, "");
        }

        int statusCode = responseMessage.getOperation().getStatusCode();
        String statusMessage = responseMessage.getOperation().getStatusMessage();
        NanoHTTPD.Response.IStatus status = createStatus(statusCode, statusMessage);

        byte[] body = responseMessage.hasBody() ? responseMessage.getBodyBytes() : new byte[0];
        String contentType = responseMessage.getHeaders().getFirstHeader("Content-Type");
        if (contentType == null) {
            contentType = NanoHTTPD.MIME_PLAINTEXT;
        }

        NanoHTTPD.Response response;
        if (body.length > 0) {
            response = NanoHTTPD.newFixedLengthResponse(status, contentType, new ByteArrayInputStream(body),
                    body.length);
        } else {
            response = NanoHTTPD.newFixedLengthResponse(status, contentType, "");
        }

        addResponseHeaders(response, responseMessage.getHeaders());
        return response;
    }

    private void addResponseHeaders(NanoHTTPD.Response response, UpnpHeaders headers) {
        if (headers == null || headers.isEmpty()) {
            return;
        }
        for (String headerName : headers.keySet()) {
            if ("Content-Type".equalsIgnoreCase(headerName)) {
                continue;
            }
            List<String> values = headers.get(headerName);
            if (values == null) {
                continue;
            }
            for (String value : values) {
                response.addHeader(headerName, value);
            }
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
