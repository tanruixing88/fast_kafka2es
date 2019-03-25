package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tanruixing  
 * Created on 2019-03-21
 */
public interface ProcMsg {
    Logger logger = LoggerFactory.getLogger(ProcMsg.class);
    public void input2output(byte[] msg, ES output);
}
