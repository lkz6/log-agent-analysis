package com.hzx.elastic;

import com.google.gson.Gson;
import com.hzx.elastic.config.HttpClientConfig;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.ExceptionEvent;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.nio.reactor.IOReactorExceptionHandler;
import org.apache.http.pool.PoolStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JestClientFactory {
   private   final static Logger log =
            LoggerFactory.getLogger(JestClientFactory.class);
    private HttpClientConfig httpClientConfig;
    private PoolingNHttpClientConnectionManager asyncConnectionManager;
    private static final int HTTPCLIENTCONNECTIONMANAGER_CLOSEWAITTIME_MS = 1000;
    private static final int HTTPCLIENTCONNECTIONMANAGER_CLOSEIDLETIME_S = 30;
    private volatile ConnectionMonitor connectionMonitor;

    public JestHttpClient getObject() {
        JestHttpClient client = new JestHttpClient();

        if (httpClientConfig == null) {
            log.debug("There is no configuration to create http client. Going to create simple client with default values");
            httpClientConfig = new HttpClientConfig.Builder("http://localhost:9200").build();
        }

        client.setRequestCompressionEnabled(httpClientConfig.isRequestCompressionEnabled());
        client.setServers(httpClientConfig.getServerList());

        HttpClientConnectionManager connectionManager  = getConnectionManager();
//        IdleConnectionEvictor.addConnectionManager(connectionManager);
        client.setHttpClient(createHttpClient(connectionManager));


        asyncConnectionManager = getAsyncConnectionManager();
        if (connectionMonitor == null) {
            connectionMonitor = new ConnectionMonitor();
            connectionMonitor.start();
        }
        connectionMonitor.addConnectionManager(asyncConnectionManager);
        client.setAsyncClient(createAsyncHttpClient(asyncConnectionManager));

        Gson gson = httpClientConfig.getGson();
        if (gson == null) {
            log.info("Using default GSON instance");
        } else {
            log.info("Using custom GSON instance");
            client.setGson(gson);
        }



        return client;
    }

    public void setHttpClientConfig(HttpClientConfig httpClientConfig) {
        this.httpClientConfig = httpClientConfig;
    }

    private CloseableHttpClient createHttpClient(HttpClientConnectionManager connectionManager) {
        return configureHttpClient(
                HttpClients.custom()
                        .setConnectionManager(connectionManager)
                        .setDefaultRequestConfig(getRequestConfig())
                        .setProxyAuthenticationStrategy(httpClientConfig.getProxyAuthenticationStrategy())
                        .setRoutePlanner(getRoutePlanner())
                        .setDefaultCredentialsProvider(httpClientConfig.getCredentialsProvider())
        ).build();
    }


    /**
     * Extension point
     * <p>
     * Example:
     * </p>
     * <pre>
     * final JestClientFactory factory = new JestClientFactory() {
     *    {@literal @Override}
     *  	protected HttpClientBuilder configureHttpClient(HttpClientBuilder builder) {
     *  		return builder.setDefaultHeaders(...);
     *    }
     * }
     * </pre>
     */
    protected HttpClientBuilder configureHttpClient(final HttpClientBuilder builder) {
        return builder;
    }

    /**
     * Extension point for async client
     */


    // Extension point
    protected HttpRoutePlanner getRoutePlanner() {
        return httpClientConfig.getHttpRoutePlanner();
    }

    // Extension point
    protected RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setConnectTimeout(httpClientConfig.getConnTimeout())
                .setSocketTimeout(httpClientConfig.getReadTimeout())
                .build();
    }


    // Extension point
    protected HttpClientConnectionManager getConnectionManager() {
        HttpClientConnectionManager retval;

        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", httpClientConfig.getPlainSocketFactory())
                .register("https", httpClientConfig.getSslSocketFactory())
                .build();

        if (httpClientConfig.isMultiThreaded()) {
            log.info("Using multi thread/connection supporting pooling connection manager");
            final PoolingHttpClientConnectionManager poolingConnMgr = new PoolingHttpClientConnectionManager(registry);

            final Integer maxTotal = httpClientConfig.getMaxTotalConnection();
            if (maxTotal != null) {
                poolingConnMgr.setMaxTotal(maxTotal);
            }
            final Integer defaultMaxPerRoute = httpClientConfig.getDefaultMaxTotalConnectionPerRoute();
            if (defaultMaxPerRoute != null) {
                poolingConnMgr.setDefaultMaxPerRoute(defaultMaxPerRoute);
            }
            final Map<HttpRoute, Integer> maxPerRoute = httpClientConfig.getMaxTotalConnectionPerRoute();
            for (Map.Entry<HttpRoute, Integer> entry : maxPerRoute.entrySet()) {
                poolingConnMgr.setMaxPerRoute(entry.getKey(), entry.getValue());
            }
            retval = poolingConnMgr;
        } else {
            log.info("Using single thread/connection supporting basic connection manager");
            retval = new BasicHttpClientConnectionManager(registry);
        }

        return retval;
    }

    protected PoolingNHttpClientConnectionManager getAsyncConnectionManager() {
        PoolingNHttpClientConnectionManager retval;

        IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setConnectTimeout(httpClientConfig.getConnTimeout())
                .setSoTimeout(httpClientConfig.getReadTimeout())
                .setIoThreadCount(httpClientConfig.getIoReactor())
                .build();

        Registry<SchemeIOSessionStrategy> sessionStrategyRegistry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                .register("http", httpClientConfig.getHttpIOSessionStrategy())
                .register("https", httpClientConfig.getHttpsIOSessionStrategy())
                .build();

        try {
            DefaultConnectingIOReactor defaultConnectingIOReactor = new DefaultConnectingIOReactor(ioReactorConfig);

            defaultConnectingIOReactor.setExceptionHandler(new IOReactorExceptionHandler() {
                @Override
                public boolean handle(IOException e) {
                    e.printStackTrace();
                    return true;
                }

                @Override
                public boolean handle(RuntimeException e) {
                    return false;
                }
            });
            retval = new PoolingNHttpClientConnectionManager(
                    defaultConnectingIOReactor,
                    sessionStrategyRegistry
            );

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                final List<ExceptionEvent> events = defaultConnectingIOReactor.getAuditLog();
                if (events != null)
                    events.stream().filter(event -> event != null).forEach(event -> {
                        log.info(
                                "Apache Async HTTP Client I/O Reactor Error Time: " + event.getTimestamp());
                        //noinspection ThrowableResultOfMethodCallIgnored
                        if (event.getCause() != null){
                            //noinspection ThrowableResultOfMethodCallIgnored
                            log.info("报错信息: ");
                            event.getCause().printStackTrace();
                        }
                    });
            }));
        } catch (IOReactorException e) {
            throw new IllegalStateException(e);
        }

        final Integer maxTotal = httpClientConfig.getMaxTotalConnection();
        if (maxTotal != null) {
            retval.setMaxTotal(maxTotal);
        }
        final Integer defaultMaxPerRoute = httpClientConfig.getDefaultMaxTotalConnectionPerRoute();
        if (defaultMaxPerRoute != null) {
            retval.setDefaultMaxPerRoute(defaultMaxPerRoute);
        }
        final Map<HttpRoute, Integer> maxPerRoute = httpClientConfig.getMaxTotalConnectionPerRoute();
        for (Map.Entry<HttpRoute, Integer> entry : maxPerRoute.entrySet()) {
            retval.setMaxPerRoute(entry.getKey(), entry.getValue());
        }

        return retval;
    }


    private CloseableHttpAsyncClient createAsyncHttpClient(NHttpClientConnectionManager connectionManager) {
        return configureHttpClient(
                HttpAsyncClients.custom()
                        .setConnectionManager(connectionManager)
                        .setDefaultRequestConfig(getRequestConfig())
                        .setProxyAuthenticationStrategy(httpClientConfig.getProxyAuthenticationStrategy())
                        .setRoutePlanner(getRoutePlanner())
                        .setDefaultCredentialsProvider(httpClientConfig.getCredentialsProvider())
                        .setConnectionManagerShared(false)
                .setKeepAliveStrategy(new HttpClientKeepAliveStrategy())
        ).build();
    }

    protected HttpAsyncClientBuilder configureHttpClient(final HttpAsyncClientBuilder builder) {
        return builder;
    }

    protected class ConnectionMonitor extends Thread {
        private volatile boolean shutdown;
        private final List<PoolingNHttpClientConnectionManager> connectionManagers = Collections.synchronizedList(new LinkedList<>());

        public void addConnectionManager(PoolingNHttpClientConnectionManager connectionManager) {
            connectionManagers.add(connectionManager);
        }

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    synchronized (this) {
                        wait(HTTPCLIENTCONNECTIONMANAGER_CLOSEWAITTIME_MS);
                        for (PoolingNHttpClientConnectionManager connectionManager : connectionManagers) {
                            PoolStats totalStats = connectionManager.getTotalStats();
                            log.info("available: {}, pending: {},max: {}, lease: {} ",totalStats.getAvailable(),totalStats.getPending(),totalStats.getMax(),totalStats.getLeased());
                            connectionManager.closeExpiredConnections();
                            connectionManager.closeIdleConnections(HTTPCLIENTCONNECTIONMANAGER_CLOSEIDLETIME_S, TimeUnit.SECONDS);
                        }
                    }
                }
            } catch (InterruptedException ex) {
                shutdown();
            }
        }

        public void shutdown() {
            shutdown = true;
            connectionManagers.clear();
            synchronized (this) {
                notifyAll();
            }
        }
    }

}
