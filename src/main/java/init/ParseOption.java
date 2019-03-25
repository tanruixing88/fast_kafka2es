package init;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tanruixing  
 * Created on 2019-03-20
 */
public class ParseOption {
    private static final Logger logger = LoggerFactory.getLogger(ParseOption.class);
    private String confFileStr;

    public ParseOption(String[] args) {
        try {
            Options ops = new Options();
            Option op = new Option("c", "config-file", true, "specify a yaml config file");
            op.setRequired(true);
            ops.addOption(op);
            CommandLineParser parser = new GnuParser();
            CommandLine commandLine = parser.parse(ops, args);
            if (commandLine.hasOption("c")) {
                confFileStr = commandLine.getOptionValue("c");
            }
        } catch (ParseException e) {
            logger.error("parse.parse error:{}", e);
            System.exit(-1);
        }
    }

    public String getConfFileStr() {
        return confFileStr;
    }
}
