package com.hzx.elastic;

import com.hzx.common.TaskManager;
import com.hzx.elastic.config.HttpClientConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JestManager {
    private final static Logger log =
            LoggerFactory.getLogger(JestManager.class);

    private Map<String, String> config;

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    private JestHttpClient jestHttpClient;

    private JestManager() {
    }

    public void initJestClient() {

//        if(MapUtils.isEmpty(config)){
//            log.info("配置为空");
//            return;
//        }
        String hosts = config.getOrDefault("es.client", "localhost:9200");
        String username = config.getOrDefault("es.username", "");
        String password = config.getOrDefault("es.password", "");
        int maxTotalConnection = Integer.parseInt(config.getOrDefault(
                "maxTotalConnection"
                , "30"));
        int maxTotalConnectionPerRoute = Integer.parseInt(config.getOrDefault(
                "maxTotalConnectionPerRoute"
                , "10"));
        int connTimeout = Integer.parseInt(config.getOrDefault(
                "connTimeout"
                , "60000"));
        int ioReactor = Integer.parseInt(config.getOrDefault(
                "ioReactor"
                , "10"));
        int readTimeout = Integer.parseInt(config.getOrDefault(
                "readTimeout"
                , "60000"));
        JestClientFactory factory = new JestClientFactory();
        try {
            String[] hostList = hosts.split(",");
            List<String> notes = new LinkedList<String>();
            for (String host : hostList) {
                if (host.startsWith("http")) {
                    URL url = new URL(host.trim());
                    notes.add("http://"+url.getHost() + ":" + url.getPort());
                } else {
                    String[] parts = host.trim().split(":", 2);
                    if (parts.length > 1) {
                        notes.add("http://"+parts[0] + ":" + parts[1]);
                    } else {
                        throw new MalformedURLException("invalid elasticsearch hosts format");
                    }
                }

                /**
                 * connectionRequestTimeout:从连接池中获取连接的超时时间，超过该时间未拿到可用连接，会抛出org.apache.http.conn.ConnectionPoolTimeoutException: Timeout waiting for connection from pool
                 * connectTimeout:连接上服务器(握手成功)的时间，超出该时间抛出connect timeout
                 * socketTimeout:服务器返回数据(response)的时间，超过该时间抛出read timeout
                 *
                 */
                HttpClientConfig.Builder httpClientConfig = new HttpClientConfig
                        .Builder(notes)
                        .requestCompressionEnabled(true)
                        .connTimeout(connTimeout) // url的连接等待时间,比如连接百度
                        .readTimeout(readTimeout)// 读取数据时间
                        .multiThreaded(true)
                        .ioReactor(ioReactor)
                        .discoveryEnabled(true)
                        .maxTotalConnection(maxTotalConnection)
                        .defaultMaxTotalConnectionPerRoute(maxTotalConnectionPerRoute)
                        .discoveryFrequency(15000L, TimeUnit.MILLISECONDS);

                if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
                    httpClientConfig.defaultCredentials(username, password);
                }
                factory.setHttpClientConfig(httpClientConfig.build());
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("初始化jestClient实例失败:", e);
        }
        this.jestHttpClient = factory.getObject();
    }


    public CloseableHttpResponse execute(Actions actions) {
        getJestHttpClient();
        CloseableHttpResponse response = null;
        try {
            response = jestHttpClient.execute(actions);
        } catch (IOException e) {
            e.printStackTrace();
            log.info("写入Elasticsearch失败,重新加入写入队列",e);
            TaskManager.addTask(actions);
        }
        return response;
    }

    public void  asyncExecute(Actions actions) {
        getJestHttpClient();
        jestHttpClient.executeAsync(actions);

    }

    private synchronized void getJestHttpClient() {
        if (jestHttpClient == null) {
            JestManager jestManager = getInstance();
            jestManager.initJestClient();
        }
    }

    /**
     * 单例模式
     */

    public static JestManager getInstance() {
        return SingleInstance.JEST_MANAGER;
    }

    public void close() {
        if (jestHttpClient != null) {
            try {
                jestHttpClient.close();
                log.info("关闭http client");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static class SingleInstance {
        private static final JestManager JEST_MANAGER = new JestManager();
    }


    public static void main(String[] args) {
        JestManager jestManager = JestManager.getInstance();
        jestManager.setConfig(new HashMap<>());
        jestManager.initJestClient();
    }


}
