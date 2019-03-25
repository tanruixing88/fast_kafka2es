package config;

import dao.YamlDao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author tanruixing  
 * Created on 2019-03-20
 */
public class Config {
    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    private YamlDao yamlDao;

    private String brokers;
    private String zookeepers;
    private String group;
    private String topics;
    private List<String> topicList = new ArrayList<>();
    private List<String> brokerList = new ArrayList<>();
    private HashMap<String, String> groupConfigHashMap = new HashMap<>();

    private int worker_threads;

    private String esClusterName;
    private List<String> httpHostPortList = new ArrayList<>();
    private String indexFormat;
    private List<String> indexFormatArgs = new ArrayList<>();
    private String typeFormat;
    private List<String> typeFormatArgs = new ArrayList<>();
    private String idFormat;
    private List<String> idFormatArgs = new ArrayList<>();
    private String routingFormat;
    private List<String> routingFormatArgs = new ArrayList<>();

    private int bulkIndexSize;
    private boolean force_utc = false;
    private String time_zone = "utc";
    private String time_key = "";
    private String time_fmt = "";

    private String format = "json";

    public Config(String configFileStr) {
        yamlDao = new YamlDao(configFileStr);
        Map map = yamlDao.getMap();
        parseConfigMap(map);
    }


    public Map getConfigMap() {
        return yamlDao.getMap();
    }

    public void parseConfigMap(Map map) {
        Map input = (Map) map.get("input");
        Map kafka = (Map) input.get("kafka");
        this.brokers = (String) kafka.get("brokers");
        String[] brokerStrList = this.brokers.split(",");
        for (String brokerStr : brokerStrList) {
            brokerList.add(brokerStr);
        }
        this.zookeepers = (String) kafka.get("zookeepers");
        this.group = (String) kafka.get("group");
        this.topics = (String) kafka.get("topics");
        if (kafka.get("group_config_list") != null) {
            for (HashMap<String, String> group_config_hashmap : (List<HashMap>)kafka.get("group_config_list")) {
                for (Map.Entry<String, String> entry : group_config_hashmap.entrySet()) {
                    this.groupConfigHashMap.put(entry.getKey(), entry.getValue());
                }
            }
        } else {
            logger.info("kafka group config list not config!");
        }

        String[] topicStrList = this.topics.split(",");
        for (String topicStr : topicStrList) {
            this.topicList.add(topicStr);
        }

        Map process = (Map) map.get("process");
        this.worker_threads = (int)process.get("worker_threads");

        Map output = (Map) map.get("output");
        Map es = (Map) output.get("elasticsearch");
        this.esClusterName = (String)es.get("cluster_name");
        this.httpHostPortList = (List<String>)es.get("http_host_port_list");
        this.indexFormat = (String)es.get("index_format");
        this.indexFormatArgs = (List<String>)es.get("index_format_args");
        this.typeFormat = (String)es.get("type_format");
        this.typeFormatArgs = (List<String>)es.get("type_format_args");
        this.idFormat = (String)es.get("id_format");
        this.idFormatArgs = (List<String>)es.get("id_format_args");
        this.routingFormat = (String)es.get("routing_format");
        this.routingFormatArgs = (List<String>)es.get("routing_format_args");
        this.bulkIndexSize = (int)es.get("bulk_index_size");
        Map custom = (Map)es.get("custom");
        if (custom != null) {
            this.force_utc = (boolean)custom.get("force_utc");
            this.time_zone = (String)custom.get("time_zone");
            this.time_key = (String)custom.get("time_key");
            this.time_fmt = (String)custom.get("time_fmt");
        }

        logger.info("config parse ret:{}", this.toString());
    }

    public String getBrokers() {
        return brokers;
    }

    public List<String> getBrokerList() {
        return this.brokerList;
    }

    public String selectOneBroker() {
        return this.brokerList.get(0);
    }

    public String getZookeepers() {
        return zookeepers;
    }

    public String getGroup() {
        return group;
    }

    public String getTopics() {
        return topics;
    }

    public List<String> getTopicList() {
        return topicList;
    }

    public String getFormat() {
        return format;
    }

    public int getWorker_threads() {
        return this.worker_threads;
    }

    public HashMap<String, String> getGroupConfigHashMap() {
        return this.groupConfigHashMap;
    }

    public String getEsClusterName() {
        return this.esClusterName;
    }

    public List<String> getHttpHostPortList() {
        return this.httpHostPortList;
    }

    public String getIndexFormat() {
        return this.indexFormat;
    }

    public List<String> getIndexFormatArgs() {
        return indexFormatArgs;
    }

    public String getTypeFormat() {
        return typeFormat;
    }

    public List<String> getTypeFormatArgs() {
        return typeFormatArgs;
    }

    public String getIdFormat() {
        return idFormat;
    }

    public List<String> getIdFormatArgs() {
        return idFormatArgs;
    }

    public String getRoutingFormat() {
        return routingFormat;
    }

    public List<String> getRoutingFormatArgs() {
        return routingFormatArgs;
    }

    public int getBulkIndexSize() {
        return this.bulkIndexSize;
    }

    public boolean isForce_utc() {
        return this.force_utc;
    }

    public String getTime_zone() {
        return this.time_zone;
    }

    public String getTime_key() {
        return this.time_key;
    }

    public String getTime_fmt() {
        return this.time_fmt;
    }

    public String toString() {
        for (Map.Entry<String, String> entry : this.groupConfigHashMap.entrySet()) {
            logger.info("group config list key:{} value:{}", entry.getKey(), entry.getValue());
        }

        return "kafka brokers:" + this.brokers + " zookeepers:" + this.zookeepers
                + " group:" + this.group + " topic:" + this.topics + " worker_threads:" + this.worker_threads;
    }
}
