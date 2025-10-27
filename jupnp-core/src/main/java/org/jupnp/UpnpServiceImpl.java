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
package org.jupnp;

import org.jupnp.controlpoint.ControlPoint;
import org.jupnp.controlpoint.ControlPointImpl;
import org.jupnp.model.message.header.STAllHeader;
import org.jupnp.protocol.ProtocolFactory;
import org.jupnp.protocol.ProtocolFactoryImpl;
import org.jupnp.registry.Registry;
import org.jupnp.registry.RegistryImpl;
import org.jupnp.transport.Router;
import org.jupnp.transport.RouterException;
import org.jupnp.transport.RouterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight {@link UpnpService} implementation suitable for non-OSGi environments.
 */
public class UpnpServiceImpl implements UpnpService {

    private final Logger logger = LoggerFactory.getLogger(UpnpServiceImpl.class);

    private UpnpServiceConfiguration configuration;
    private ProtocolFactory protocolFactory;
    private Registry registry;
    private ControlPoint controlPoint;
    private Router router;

    private volatile boolean isRunning;

    public UpnpServiceImpl() {
        this(null);
    }

    public UpnpServiceImpl(UpnpServiceConfiguration configuration) {
        this.configuration = configuration != null ? configuration : new DefaultUpnpServiceConfiguration();
        startup();
    }

    @Override
    public synchronized void startup() {
        if (isRunning) {
            return;
        }
        logger.debug("Starting UPnP service");
        try {
            protocolFactory = createProtocolFactory();
            registry = createRegistry(protocolFactory);
            router = createRouter(protocolFactory, registry);
            controlPoint = createControlPoint(protocolFactory, registry);

            router.enable();
            controlPoint.search(new STAllHeader());
            isRunning = true;
        } catch (Exception ex) {
            logger.error("Failed to start UPnP service", ex);
            shutdown();
            throw new RuntimeException("Could not start UPnP service", ex);
        }
    }

    @Override
    public synchronized void shutdown() {
        if (!isRunning) {
            return;
        }
        logger.debug("Shutting down UPnP service");
        isRunning = false;
        if (controlPoint != null) {
            controlPoint = null;
        }
        if (router != null) {
            try {
                router.shutdown();
            } catch (RouterException ex) {
                logger.warn("Router shutdown failed", ex);
            }
            router = null;
        }
        if (registry != null) {
            registry.shutdown();
            registry = null;
        }
        if (protocolFactory != null) {
            protocolFactory = null;
        }
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Override
    public UpnpServiceConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public ControlPoint getControlPoint() {
        return controlPoint;
    }

    @Override
    public ProtocolFactory getProtocolFactory() {
        return protocolFactory;
    }

    @Override
    public Registry getRegistry() {
        return registry;
    }

    @Override
    public Router getRouter() {
        return router;
    }

    protected ProtocolFactory createProtocolFactory() {
        return new ProtocolFactoryImpl(this);
    }

    protected Registry createRegistry(ProtocolFactory protocolFactory) {
        return new RegistryImpl(this);
    }

    protected Router createRouter(ProtocolFactory protocolFactory, Registry registry) {
        return new RouterImpl(getConfiguration(), protocolFactory);
    }

    protected ControlPoint createControlPoint(ProtocolFactory protocolFactory, Registry registry) {
        return new ControlPointImpl(getConfiguration(), protocolFactory, registry);
    }
}
