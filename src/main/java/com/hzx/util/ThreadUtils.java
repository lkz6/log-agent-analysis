package com.hzx.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ThreadUtils {
    private ThreadUtils() {

    }

    public static ThreadFactory threadFactory(String threadName, boolean isDaemon) {
        return new ThreadFactoryBuilder()
                .setNameFormat(threadName + "-%d")
                .setDaemon(isDaemon)
                .build();
    }

    public static ThreadFactory threadFactory(String threadName) {
        return threadFactory(threadName, false);
    }

    public static void shutdownExecutorService(ExecutorService executorService) throws InterruptedException {
        shutdownExecutorService(executorService, 5);
    }

    public static void shutdownExecutorService(ExecutorService executorService, int timeoutS) throws InterruptedException {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            if (!executorService.awaitTermination(timeoutS, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                executorService.awaitTermination(timeoutS, TimeUnit.SECONDS);
            }
        }
    }
}
