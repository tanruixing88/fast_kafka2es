package core;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import config.Config;
import util.Common;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * @author tanruixing  
 * Created on 2019-03-20
 */
public class ES {
    private static Logger logger = LoggerFactory.getLogger(Thread.currentThread().getName());

    private static String clusterName;
    private static List<HttpHost> httpHostList = new ArrayList<>();
    private static String indexFormat;
    private static List<String> indexFormatArgs = null;
    private static String typeFormat;
    private static List<String> typeFormatArgs = null;
    private static String idFormat;
    private static List<String> idFormatArgs = null;
    private static String routingFormat;
    private static List<String> routingFormatArgs = null;
    private static int bulkIndexSize;
    private BulkProcessor bulkProcessor;
    private RestHighLevelClient client;
    private static boolean force_utc = false;
    private static boolean utcFlag = false;
    private static String time_zone = "utc";
    private static String time_key = "null";
    private static String time_fmt = "null";

    private static ConcurrentHashMap<String, Long> indexNameFormatCHM = new ConcurrentHashMap<>();
    private static final Long INDEX_NAME_UPDATE_TIME_INTERVAL = 12 * 3600 * 1000l;
    private static final Long INDEX_NAME_DISUSE_TIME = INDEX_NAME_UPDATE_TIME_INTERVAL + 12 * 3600 * 1000l;
    private static String indexType = "day";


    public ES() {
    }

    public ES(Config config) {
        clusterName = config.getEsClusterName();

        for(String httpHostPort : config.getHttpHostPortList()) {
            String httpHost = httpHostPort.split(":")[0];
            Integer httpPort = Integer.parseInt(httpHostPort.split(":")[1]);
            httpHostList.add(new HttpHost(httpHost, httpPort));
        }

        indexFormat = config.getIndexFormat();
        indexFormatArgs = config.getIndexFormatArgs();
        if (indexFormatArgs == null || indexFormatArgs.size() == 0) {
            indexFormatArgs = null;
        }

        typeFormat = config.getTypeFormat();
        typeFormatArgs = config.getTypeFormatArgs();
        if (typeFormatArgs == null || typeFormatArgs.size() == 0) {
            typeFormatArgs = null;
        }

        idFormat = config.getIdFormat();
        idFormatArgs = config.getIdFormatArgs();
        if (idFormatArgs == null || idFormatArgs.size() == 0) {
            idFormatArgs = null;
        }

        routingFormat = config.getRoutingFormat();
        routingFormatArgs = config.getRoutingFormatArgs();
        if (routingFormatArgs == null || routingFormatArgs.size() == 0) {
            routingFormatArgs = null;
        }

        bulkIndexSize = config.getBulkIndexSize();
        force_utc = config.isForce_utc();
        time_zone = config.getTime_zone();
        time_key = config.getTime_key();
        time_fmt = config.getTime_fmt();


        if (indexFormat.contains("yyyy.MM.dd.HH")) {
            indexType = "hour";
        }
    }

    private static RestHighLevelClient getRestHighLevelClient() {
        HttpHost[] httpHosts = httpHostList.toArray(new HttpHost[httpHostList.size()]);
        RestClientBuilder builder = RestClient.builder(httpHosts);
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public void initBulkProcessor() {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                logger.info("start bulk_request_id:{} request_size:{}", l, bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                logger.info("done bulk_request_id:{} request_size:{}", l, bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                throwable.printStackTrace();
                logger.info("fail bulk_request_id:{} request_size:{} error:{}", l, bulkRequest.numberOfActions(), throwable.getMessage());
            }
        };

        client = getRestHighLevelClient();
        bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener)
                .setBulkActions(bulkIndexSize)
                .setBulkSize(new ByteSizeValue(50, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(30))
                .setConcurrentRequests(12)
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(10l), Integer.MAX_VALUE))
                .build();
    }

    public void close() {
        bulkProcessor.close();
        try {
            client.close();
        } catch (IOException e) {
            logger.error("close client error:{}", e);
        }
    }

    public void indexDoc2ES(String indexName, String typeName, String docId, String routing, String source) {
        if (docId == null) {
            if (routing == null) {
                bulkProcessor.add(new IndexRequest(indexName, typeName).source(source, XContentType.JSON));
            } else {
                bulkProcessor.add(new IndexRequest(indexName, typeName).routing(routing).source(source, XContentType.JSON));
            }
        } else {
            if (routing == null) {
                bulkProcessor.add(new IndexRequest(indexName, typeName, docId).source(source, XContentType.JSON));
            } else {
                bulkProcessor.add(new IndexRequest(indexName, typeName, docId).routing(routing).source(source, XContentType.JSON));
            }
        }
    }

    public DateTime getDocDatetime(JSONObject doc) {
        DateTime dt;
        if (doc.get("@timestamp") != null) {
            dt = new DateTime(doc.get("@timestamp"), DateTimeZone.UTC);
            if (dt.getMillis() - System.currentTimeMillis() > 7 * 3600 * 1000) {
                utcFlag = true;
            }

            if (force_utc && utcFlag) {
                dt = new DateTime(dt.getMillis() - 8 * 3600 * 1000);
            }

            return dt;
        }

        long timestamp = 0;
        if (doc.get("time") != null && Common.isNumeric(doc.get("time").toString())) {
            timestamp = (long)doc.get("time");
        } else if (doc.get("timestamp") != null && Common.isNumeric(doc.get("timestamp").toString())) {
            timestamp = (long)doc.get("timestamp");
        } else if (!time_key.equals("null") && !time_key.equals("null")) {
            String time_val = (String)doc.get(time_key);
            if (time_val != null) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(time_fmt);
                try {
                    timestamp = simpleDateFormat.parse(time_val).getTime();
                } catch (java.text.ParseException e) {
                    logger.error("parse time_fmt:{} time_val:{} error:{}", time_fmt, time_val, e);
                }
            }
        } else {
            timestamp = System.currentTimeMillis();
        }

        if (time_zone.equals("utc") || force_utc) {
            dt = new DateTime(timestamp, DateTimeZone.UTC);
        } else {
            dt = new DateTime(timestamp);
        }

        if (timestamp % 10000 == 0) {
            logger.info("time_zone:{} dt:{} doc:{}", DateTimeZone.getDefault(), dt.toString(), doc.toJSONString());
        }

        return dt;
    }

    public String getNameByArg(JSONObject doc, String format, List<String> formatArgs) {
        if (formatArgs != null) {
            List<Object> formatArgVals = new ArrayList<>();
            for (String formatArg : formatArgs) {
                formatArgVals.add(doc.get(formatArg));
            }
            return String.format(format, formatArgVals.toArray());
        } else {
            return format;
        }
    }



    public String getIndexName(JSONObject doc, DateTime dt) {
        String indexTimeFormat = getNameByArg(doc, indexFormat, indexFormatArgs);
        Long indexNameTimestamp = indexNameFormatCHM.get(indexTimeFormat);
        if (indexNameTimestamp == null || dt.getMillis() - indexNameTimestamp > INDEX_NAME_UPDATE_TIME_INTERVAL) {
            indexNameFormatCHM.putIfAbsent(indexTimeFormat, dt.getMillis());
        }

        String indexName = Common.getIndexNameByFmtDt(indexTimeFormat, dt);
        return indexName;
    }

    public String getTypeName(JSONObject doc) {
        return getNameByArg(doc, typeFormat, typeFormatArgs);
    }

    public String getDocItem(JSONObject doc, String format, List<String> formatArgs) {
        if (formatArgs == null) {
            return null;
        }

        List<Object> formatArgVals = new ArrayList<>();
        for (String formatArg : formatArgs) {
            formatArgVals.add(doc.get(formatArg));
        }
        String name = String.format(format, formatArgVals.toArray());

        return name;
    }

    public String getDocId(JSONObject doc) {
        return getDocItem(doc, idFormat, idFormatArgs);
    }

    public String getRouting(JSONObject doc) {
        return getDocItem(doc, routingFormat, routingFormatArgs);
    }

    public void procDoc(JSONObject doc) {
        if (doc == null) {
            return;
        }

        DateTime dt = getDocDatetime(doc);
        doc.put("@timestamp", dt.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));

        String indexName = getIndexName(doc, dt);
        String typeName = getTypeName(doc);
        String id = getDocId(doc);
        String routing = getRouting(doc);
        indexDoc2ES(indexName, typeName, id, routing, doc.toJSONString());
    }

    public static void createIndexIfAbsent(String indexName) {
        RestHighLevelClient client = getRestHighLevelClient();

        try {
            GetIndexRequest getIndexRequest = new GetIndexRequest().indices(indexName);
            boolean indexExistFlag = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            if (indexExistFlag) {
                logger.info("index:{} has created.", indexName);
                return;
            }

            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("PUT", "/" + indexName);
            Response response = restClient.performRequest(request);
            if (response.getStatusLine().getStatusCode() == 200) {
                logger.info("create indexName:{} success!", indexName);
            }
        } catch (IOException e) {
            logger.error("create indexName:{} fail, error:{}", indexName, e);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                logger.error("createIndex indexName:{} client close error:{}", indexName, e);
            }
        }
    }

    public static List<String> getNextIndexNameList() {
        List<String> nextIndexNameList = new ArrayList<>();
        Long currentTimestamp = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : indexNameFormatCHM.entrySet()) {
            if (currentTimestamp - entry.getValue() > INDEX_NAME_DISUSE_TIME) {
                logger.info("the indexName:{} has disused and last update timestamp:{} check time:{}",
                        entry.getKey(), entry.getValue(), currentTimestamp);
            } else {
                DateTime nextDt;
                if (time_zone.equals("utc") || force_utc) {
                    if (indexType.equals("day")) {
                        nextDt = new DateTime(currentTimestamp + (24 * 60 * 60 * 1000l), DateTimeZone.UTC);
                    } else {
                        nextDt = new DateTime(currentTimestamp + (1 * 60 * 60 * 1000l), DateTimeZone.UTC);
                    }
                } else {
                    if (indexType.equals("day")) {
                        nextDt = new DateTime(currentTimestamp + (24 * 60 * 60 * 1000l), DateTimeZone.getDefault());
                    } else {
                        nextDt = new DateTime(currentTimestamp + (1 * 60 * 60 * 1000l), DateTimeZone.getDefault());
                    }
                }

                String nextIndexName = Common.getIndexNameByFmtDt(entry.getKey(), nextDt);
                nextIndexNameList.add(nextIndexName);
            }
        }

        return nextIndexNameList;
    }

    public static void preCreateNextIndex() {
        List<String> nextIndexNameList = getNextIndexNameList();
        logger.info("preCreateNextIndex  nextIndexNameList:{}", nextIndexNameList.toArray());
        for (String nextIndexName : nextIndexNameList) {
            createIndexIfAbsent(nextIndexName);
        }
    }
}
