package com.hzx.sink;

import com.hzx.common.FileCloseExecutor;
import com.hzx.common.TaskManager;
import com.hzx.elastic.Bulk;
import com.hzx.elastic.IndexAction;
import com.hzx.util.ThreadUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

public class SinkManager {
    private final static Logger log =
            LoggerFactory.getLogger(SinkManager.class);

    // key: index，value: data
    private  Map<String, Bulk> listMap = new ConcurrentHashMap<>();
    /**
     * 一个批量写出包括多少条数据
     */
    private final Integer count;
    private  final  int timeout;
    private ExecutorService service;
    private ScheduledExecutorService executorService;

    public SinkManager(Map<String, String> map) {

        this.count = Integer.parseInt(map.getOrDefault("count", "5000"));
        this.timeout = Integer.parseInt(map.getOrDefault("timeout", "60"));
        ThreadFactory threadFactory = ThreadUtils.threadFactory("handlerCheck");
        executorService = Executors.newScheduledThreadPool(1, threadFactory);
        CheckThread checkHandlesThread = new CheckThread();
        executorService.scheduleAtFixedRate(checkHandlesThread, timeout,
                timeout, TimeUnit.SECONDS);

        int numWriters = Integer.parseInt(map.getOrDefault("writer.threads",
                "1"));
        threadFactory = ThreadUtils.threadFactory(
                "elasticsearch" +
                        "-writer");
        service =
                Executors.newFixedThreadPool(numWriters, threadFactory);
        for (int i = 0; i < numWriters; i++) {
            FileCloseExecutor fileCloseExecutor = new FileCloseExecutor();
            service.submit(fileCloseExecutor);
        }
    }

    private Bulk buildBulk() {

        Bulk buildBulk = new Bulk();
        buildBulk.setId(RandomStringUtils.randomAlphanumeric(10));
        return buildBulk;
    }

    public  Bulk getBulk(String indexName){
        if(listMap.containsKey(indexName)){
            Bulk bulk = listMap.get(indexName);
            if(bulk != null){
                if(bulk.getActionSize() >= count){
                    TaskManager.addTask(bulk);
                    listMap.remove(indexName);
                    Bulk bulk_new = buildBulk();
                    listMap.put(indexName,bulk_new);
                    return bulk_new;
                }
                return bulk;
            }
        }
        Bulk bulk = buildBulk();
        listMap.put(indexName,bulk);
        return bulk;
    }

    private class CheckThread
            extends Thread {

        public void run() {
            if(MapUtils.isNotEmpty(listMap)) {
                for (Map.Entry<String, Bulk> bulks : listMap.entrySet()) {
                    String indexName = bulks.getKey();
                    Bulk bulk = bulks.getValue();
//                    log.info("扫描线程启动 :" + System.currentTimeMillis() / 1000);
                    if (System.currentTimeMillis() / 1000 - bulk.getLastUpdatime() >= 3L * timeout && bulk.getActionSize() > 0) {
                        log.info("扫描: bulk file created 3 minutes ago lastUpdateTime:" +
                                        " {},当前时间: {}",
                                bulk.getLastUpdatime(),
                                System.currentTimeMillis() / 1000);
                        if(listMap.get(indexName) != null){
                            TaskManager.addTask(bulk);
                            listMap.remove(indexName);
                        }

                    }
                }
            }

        }

    }

    public  void addAction(IndexAction action) {
        if(action == null){
            return;
        }
        String indexName = action.getIndexName();
        if(StringUtils.isBlank(indexName)){
            return;
        }
        Bulk bulk = getBulk(indexName);
        bulk.getbulkableActions().add(action);
        if(log.isDebugEnabled()){
            log.info("添加到bulkableActions: {}",action.getPayload());
        }
    }
//
//    public void addAction(Collection<IndexAction> actions) {
//        this.bulkableActions.addAll(actions);
//
//    }

    public void close() throws Exception {
        ThreadUtils.shutdownExecutorService(executorService);
        ThreadUtils.shutdownExecutorService(service);
    }
}
