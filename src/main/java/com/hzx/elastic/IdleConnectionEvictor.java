package com.hzx.elastic;

import org.apache.http.conn.HttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class IdleConnectionEvictor {
    static final Logger LOGGER =
            LoggerFactory.getLogger(IdleConnectionEvictor.class);

    // connection manager set
    private static final Set<HttpClientConnectionManager> connectionManagers = new HashSet<>();

    private static final long sleepTimeMs = 3000;
    private static final long maxIdleTimeMs = 3000;

    private static Thread thread;

    private static void initEvictorThread() {
        if (thread == null) {
            thread = new Thread(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(sleepTimeMs);
                        synchronized (connectionManagers) {
                            // iterate over connection managers
                            for (HttpClientConnectionManager connectionManager : connectionManagers) {
                                connectionManager.closeExpiredConnections();
                                if (maxIdleTimeMs > 0) {
                                    connectionManager.closeIdleConnections(maxIdleTimeMs, TimeUnit.MILLISECONDS);
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("Thread interrupted", e);
                    Thread.currentThread().interrupt();
                } catch (final Exception ex) {
                    LOGGER.error("移除失败",ex);
                }

            }, "Connection evictor");
//            thread.setDaemon(true);
            LOGGER.info("启动连接清理");
            thread.start();
        }
    }

    public static void shutdown() throws InterruptedException {
        thread.interrupt();
        // wait for thread to shutdown
        thread.join(sleepTimeMs);
        thread = null;
    }

    /**
     * Add connection manager to evictor thread.
     *
     * @param connectionManager connection manager
     */
    public static void addConnectionManager(HttpClientConnectionManager connectionManager) {
        synchronized (connectionManagers) {
            initEvictorThread();
            connectionManagers.add(connectionManager);
        }
    }

    /**
     * Remove connection manager from evictor thread.
     *
     * @param connectionManager connection manager
     */
    public static void removeConnectionManager(HttpClientConnectionManager connectionManager) {
        synchronized (connectionManagers) {
            connectionManagers.remove(connectionManager);
        }
    }
}
