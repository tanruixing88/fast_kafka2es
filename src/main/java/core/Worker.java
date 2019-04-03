package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * @author tanruixing  
 * Created on 2019-03-20
 */
public class Worker  implements SignalHandler {
    private static final Logger logger = LoggerFactory.getLogger("worker:" + Thread.currentThread().getName());
    private Kafka kafka;
    private ES es;
    private ProcMsg procMsg = new ProcMsg() {
        @Override
        public void input2output(byte[] msg, ES es) {
            es.procDoc(Parse.process(msg));
        }
    };

    public Worker() {
        es = new ES();
        kafka = new Kafka();
    }

    public void beforeExec() {
        kafka.initKafkaConsumer();
        es.initBulkProcessor();
    }

    public void exec() {
        kafka.consumerTopicData(es, procMsg);
    }

    public void afterExec() {

    }

    public void execute() {
        beforeExec();
        exec();
        afterExec();
    }

    public void handle(Signal signal) {
        Process.addWorkerClose();
    }
}
