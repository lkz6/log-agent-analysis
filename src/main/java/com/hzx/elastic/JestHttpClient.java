package com.hzx.elastic;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hzx.elastic.exception.CouldNotConnectException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class JestHttpClient {

    private CloseableHttpAsyncClient asyncClient;

    private final static Logger log = LoggerFactory.getLogger(JestHttpClient.class);
    public static final String ELASTIC_SEARCH_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:sssZ";

    protected Gson gson = new GsonBuilder()
            .setDateFormat(ELASTIC_SEARCH_DATE_FORMAT)
            .create();

    private boolean requestCompressionEnabled;
    private final AtomicReference<ServerPool> serverPoolReference =
            new AtomicReference<ServerPool>(new ServerPool(ImmutableSet.<String>of()));

    protected ContentType requestContentType = ContentType.APPLICATION_JSON.withCharset("utf-8");

    private CloseableHttpClient httpClient;

    private HttpClientContext httpClientContextTemplate;

    public CloseableHttpAsyncClient getAsyncClient() {
        return asyncClient;
    }

    public void setAsyncClient(CloseableHttpAsyncClient asyncClient) {
        this.asyncClient = asyncClient;
    }


    public void executeAsync(final Actions actions) {
        synchronized (this) {
            if (!asyncClient.isRunning()) {
                asyncClient.start();
            }
        }

        HttpUriRequest request = prepareRequest(actions);
        executeAsyncRequest(actions, request);
    }

    protected void executeAsyncRequest(Actions actions, HttpUriRequest request) {
        long startTime = System.currentTimeMillis();
        asyncClient.execute(request, new DefaultCallback(actions,request,startTime));
//        if (responseFuture == null) {
//            log.info("Failed to execute get request for ["
//                    + request.getURI().getPath() + "]");
//            return;
//        }
//        HttpResponse response = null;
//        try {
//            if (timeoutMillis > 0) {
//                response = responseFuture.get(timeoutMillis,
//                        TimeUnit.MILLISECONDS);
//            } else {
//                response = responseFuture.get();
//            }
//            long timeTakenMillis = System.currentTimeMillis() - startTime;
//            if (response == null) {
//                log.info("============,耗时: {}",timeTakenMillis);
//            } else {
//                int httpStatusCode = response.getStatusLine().getStatusCode();
//                if (httpStatusCode == 200) {
//                    Bulk bulk = null;
//                    if (actions instanceof Bulk) {
//                        bulk = (Bulk) actions;
//                        log.info("索引: {},写入成功,耗时: {} ms,id: {}", bulk.getbulkableActions().get(0).getIndexName(), timeTakenMillis, bulk.getId());
//                        bulk.clear();
//                    }
//                    EntityUtils.consumeQuietly(response.getEntity());
//                    bulk = null;
//                }else {
//                    log.info(response.getEntity().toString());
//                }
//            }
//        } catch (TimeoutException ex) {
//            responseFuture.cancel(true);
//            log.info("fetch for [" + request.getURI().getPath()
//                    + "] timed out", ex);
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }


    }

    protected class DefaultCallback implements FutureCallback<HttpResponse> {
        private HttpUriRequest request;
        private Actions actions;
        private long start;

        public DefaultCallback(Actions actions, HttpUriRequest request, long start) {
            this.request = request;
            this.actions = actions;
            this.start = start;
        }

        @Override
        public void completed(final HttpResponse response) {
            Bulk bulk = null;
            if (response.getStatusLine().getStatusCode() == 200) {
                if (actions instanceof Bulk) {
                    bulk = (Bulk) actions;
                    log.info("索引: {},写入成功,耗时: {} ms,id: {}", bulk.getbulkableActions().get(0).getIndexName(), (System.currentTimeMillis() - start), bulk.getId());
                }
                EntityUtils.consumeQuietly(response.getEntity());
                bulk = null;
            }
        }

        @Override
        public void failed(final Exception ex) {
//            if (actions instanceof Bulk) {
//                Bulk bulk = (Bulk) actions;
//                if(bulk != null){
//                    log.info("async failed, add to queue,id: {}",bulk.getId());
//                    TaskManager.addTask(bulk);
//                }
//            }
            log.error("Exception occurred during async execution.", ex);

        }

        @Override
        public void cancelled() {
            log.warn("Async execution was cancelled; this is not expected to occur under normal operation.");
        }
    }


    /**
     * 执行写入
     *
     * @param actions
     * @throws IOException
     */
    public CloseableHttpResponse execute(final Actions actions) throws IOException {
        HttpUriRequest request = prepareRequest(actions);
        CloseableHttpResponse response = null;
        try {
            response = executeRequest(request);
            return response;
        } catch (HttpHostConnectException ex) {
            throw new CouldNotConnectException(ex.getHost().toURI(), ex);
        }
    }


    public void shutdownClient() {
        try {
            close();
        } catch (IOException e) {
            log.error("Exception occurred while shutting down the sync client.", e);
        }
    }

    public void close() throws IOException {
        httpClient.close();
        asyncClient.close();
    }

    protected int getServerPoolSize() {
        return serverPoolReference.get().getSize();
    }

    protected String getRequestURL(String elasticSearchServer, String uri) {
        StringBuilder sb = new StringBuilder(elasticSearchServer);

        if (StringUtils.isBlank(elasticSearchServer)) {
            log.info("jestHttpClient http server 为空");
            System.exit(0);
        }
        if (uri.length() > 0 && uri.charAt(0) == '/') sb.append(uri);
        else sb.append('/').append(uri);

        return sb.toString();
    }

    protected HttpUriRequest prepareRequest(final Actions actions) {
        String elasticSearchRestUrl = getRequestURL(getNextServer(), actions.getURI());
        HttpUriRequest request = constructHttpMethod(actions.getRestMethodName(),
                elasticSearchRestUrl,
                actions.getData(gson));

        log.info("Request method={} url={}", actions.getRestMethodName(),
                elasticSearchRestUrl);


        return request;
    }

    protected CloseableHttpResponse executeRequest(HttpUriRequest request) throws IOException {
        return httpClient.execute(request);
    }


    protected HttpUriRequest constructHttpMethod(String methodName,
                                                 String url, String payload) {
        HttpUriRequest httpUriRequest = null;

        if (methodName.equalsIgnoreCase("POST")) {
            httpUriRequest = new HttpPost(url);
            log.debug("POST method created based on client request");
        } else if (methodName.equalsIgnoreCase("PUT")) {
            httpUriRequest = new HttpPut(url);
            log.debug("PUT method created based on client request");
        } else if (methodName.equalsIgnoreCase("HEAD")) {
            httpUriRequest = new HttpHead(url);
            log.debug("HEAD method created based on client request");
        }

        if (httpUriRequest != null && httpUriRequest instanceof HttpEntityEnclosingRequest && payload != null) {
            EntityBuilder entityBuilder = EntityBuilder.create()
                    .setText(payload)
                    .setContentType(requestContentType);


            ((HttpEntityEnclosingRequest) httpUriRequest).setEntity(entityBuilder.build());
        }

        return httpUriRequest;
    }


    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }


    public Gson getGson() {
        return gson;
    }

    protected String getNextServer() {
        return serverPoolReference.get().getNextServer();
    }

    public void setGson(Gson gson) {
        this.gson = gson;
    }

    public HttpClientContext getHttpClientContextTemplate() {
        return httpClientContextTemplate;
    }

    public void setHttpClientContextTemplate(HttpClientContext httpClientContext) {
        this.httpClientContextTemplate = httpClientContext;
    }

    Set<String> scrubServerURIs(Set<String> servers) {
        final ImmutableSet.Builder<String> scrubbedServers = ImmutableSet.builder();
        for (String server : servers) {
            final URI originalURI = URI.create(server);
            try {
                final URI scrubbedURI = new URI(originalURI.getScheme(),
                        null, // Remove user info
                        originalURI.getHost(),
                        originalURI.getPort(),
                        originalURI.getPath(),
                        originalURI.getQuery(),
                        originalURI.getFragment());
                scrubbedServers.add(scrubbedURI.toString());
            } catch (URISyntaxException e) {
                log.debug("Couldn't scrub server URI " + originalURI, e);
            }
        }
        return scrubbedServers.build();
    }

    public void setServers(Set<String> servers) {

        if (log.isInfoEnabled()) {
            log.info("Setting server pool to a list of {} servers: [{}]",
                    servers.size(), Joiner.on(',').join(scrubServerURIs(servers)));
        }
        serverPoolReference.set(new ServerPool(servers));

        if (servers.isEmpty()) {
            log.warn("No servers are currently available to connect.");
        }
    }

    private static final class ServerPool {
        private final List<String> serversRing;
        private final AtomicInteger nextServerIndex = new AtomicInteger(0);

        public ServerPool(final Set<String> servers) {
            this.serversRing = ImmutableList.copyOf(servers);
        }

        public Set<String> getServers() {
            return ImmutableSet.copyOf(serversRing);
        }

        public String getNextServer() {
            if (serversRing.size() > 0) {
                try {
                    return serversRing.get(nextServerIndex.getAndIncrement() % serversRing.size());
                } catch (IndexOutOfBoundsException outOfBoundsException) {
                    // In the very rare case where nextServerIndex overflowed, this will end up with a negative number,
                    // resulting in an IndexOutOfBoundsException.
                    // We should then start back at the beginning of the server list.
                    // Note that this might happen on several threads at once, in which the reset might happen a few times
                    log.info("Resetting next server index");
                    nextServerIndex.set(0);
                    return serversRing.get(nextServerIndex.getAndIncrement() % serversRing.size());
                }
            }
            return "";
        }

        public int getSize() {
            return serversRing.size();
        }
    }

    public void setRequestCompressionEnabled(boolean requestCompressionEnabled) {
        this.requestCompressionEnabled = requestCompressionEnabled;
    }


}
