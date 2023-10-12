package com.hzx.common;

import com.hzx.elastic.Actions;
import com.hzx.elastic.Bulk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class TaskManager {

private static final Logger log =
        LoggerFactory.getLogger(TaskManager.class);

//    public static BlockingQueue<Bulk> fileCloseTaskQueue_ =
//            new LinkedBlockingQueue<>();


public static ConcurrentLinkedQueue<Actions> fileCloseTaskQueue_ =
        new ConcurrentLinkedQueue<>();

public static void addTask(Actions actions) {
    if(actions instanceof Bulk){
        Bulk bulk = (Bulk) actions;
        log.info("添加到队列,时间: {}",bulk.getLastUpdatime());
        fileCloseTaskQueue_.add(bulk);
    }
}
}