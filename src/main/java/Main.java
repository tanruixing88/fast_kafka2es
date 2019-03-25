import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import init.ParseOption;
import core.Process;
import config.Config;
import sun.misc.Signal;

/**
 * @author tanruixing  
 * Created on 2019-03-19
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("kafka to es start...");
        ParseOption objParseOption = new ParseOption(args);
        String configFile = objParseOption.getConfFileStr();
        Config config = new Config(configFile);
        Process process = new Process(config);

        Signal.handle(new Signal("TERM"), process);
        process.start();
    }
}
