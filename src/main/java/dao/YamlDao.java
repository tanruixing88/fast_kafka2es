package dao;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author tanruixing  
 * Created on 2019-03-20
 */
public class YamlDao {
    private static final Logger logger = LoggerFactory.getLogger(YamlDao.class);
    private Map map;

    public YamlDao(String yamlFileStr) {
        File dumpFile = new File(yamlFileStr);
        Yaml yaml = new Yaml();
        try {
            map = (Map) yaml.load(new FileInputStream(dumpFile));
        } catch (Exception e) {
            logger.error("yaml load error:{}", e);
            System.exit(-1);
        }
    }

    public String getMapJsonStr() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String mapJsonStr =  mapper.writeValueAsString(map);
            logger.info("current Mapper:{} mapJsonStr:{}", mapper, mapJsonStr);
            return mapJsonStr;
        } catch (JsonProcessingException e) {
            logger.error("current Mapper:{} error:{}", mapper, e);
            return "";
        }
    }

    public Map getMap() {
        return map;
    }
}
