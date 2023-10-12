package com.hzx.common;

import com.hzx.elastic.Bulk;
import com.hzx.elastic.JestManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileCloseExecutor extends Thread {
    private static final Logger log =
            LoggerFactory.getLogger(FileCloseExecutor.class);

    private final JestManager manager;

    public FileCloseExecutor() {
        log.info("初始化Elasticsearch写入线程===========");
        manager = JestManager.getInstance();
    }

    public void run() {
        Bulk bulk = null;
        try {
            while (!Thread.currentThread().isInterrupted())  {
                if (!TaskManager.fileCloseTaskQueue_.isEmpty()) {
                    bulk = (Bulk) TaskManager.fileCloseTaskQueue_.poll();
                    if (bulk != null && bulk.getActionSize() > 0) {
                        log.info("开始写入Elasticsearch,大小为: {},索引为: {},id: {}",
                                bulk.getActionSize(),
                                bulk.getbulkableActions().get(0).getIndexName(),bulk.getId());
//                        long start = System.currentTimeMillis();
                        manager.asyncExecute(bulk);
//                        CloseableHttpResponse response = manager.execute(bulk);
//                        if (response != null && response.getStatusLine().getStatusCode() == 200) {
//                            log.info("写入成功,耗时: {}",
//                                    (System.currentTimeMillis() - start) + " " +
//                                            "ms");
//                            response.close();
//                            bulk = null;
//                        }
                    }
                }
            }
        } catch (Throwable e) {
            log.error(" FileCloseExecutor 写入失败",e);

        }
    }
}
