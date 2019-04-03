package core;

import java.io.UnsupportedEncodingException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tanruixing  
 * Created on 2019-03-20
 */
public class Parse {
    private static final Logger logger = LoggerFactory.getLogger(Parse.class);

    private static String format = "json";

    public static JSONObject parseJsonMsg(byte[] msg) {
        try {
            String line = new String((byte[])msg, "utf-8");
            try {
                Object obj = new JSONParser().parse(line);
                if (obj instanceof JSONObject) {
                    return (JSONObject)obj;
                } else {
                    logger.error("unsupported can not convert JSONObject string:{}", line);
                    return null;
                }
            } catch (ParseException e) {
                logger.error("parse line error:{}", e);
                return null;
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("unsupported encoding error:{}", e);
            return null;
        }
    }

    public static JSONObject msgConvertDoc(byte[] doc) {
        switch (format) {
            case "json":
                return parseJsonMsg(doc);
        }

        return null;
    }

    public static JSONObject process(byte[] msg) {
        JSONObject doc = msgConvertDoc(msg);
        if (doc == null) {
            return null;
        }

        return doc;
    }
}
