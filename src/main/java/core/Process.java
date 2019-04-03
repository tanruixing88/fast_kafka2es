package core;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import config.Config;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * @author tanruixing  
 * Created on 2019-03-20
 */
public class Process implements SignalHandler {
    private static final Logger logger = LoggerFactory.getLogger(Process.class);
    private Config config;
    private static boolean isShutdownFlag = false;
    private static Timer preCreateNextIndexTimer = new Timer();
    private static AtomicInteger closedThreadCount = new AtomicInteger();
    private int workerThreads = 0;
    private Kafka kafka;
    private ES es;

    private ExecutorService workers;

    public Process(Config config) {
        this.config = config;
        this.workerThreads = config.getWorker_threads();
        this.kafka = new Kafka(config);
        this.es = new ES(config);
    }

    public static boolean isShutdownFlag() {
        return isShutdownFlag;
    }

    public void start() {
        logger.info("config :{}", config.toString());
        this.workers = workStart();
        preCreateNextIndex();
    }

    public ExecutorService workStart() {
        this.kafka.initKafkaProperties();
        ExecutorService executorService = Executors.newFixedThreadPool(workerThreads);
        for (int i = 0; i < workerThreads; i++) {
            executorService.execute(() -> {
                Worker worker = new Worker();
                worker.execute();
            });
        }

        return executorService;
    }

    public void preCreateNextIndex() {
        preCreateNextIndexTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                ES.preCreateNextIndex();
            }
        }, 1 * 10 * 1000L, 1 * 10 * 1000L);
    }

    public void handle(Signal signal) {
        logger.info("receive signal num:{} name:{}", signal.getNumber(), signal.getName());

        if (signal.getName().equals("TERM")) {
            isShutdownFlag = true;
            preCreateNextIndexTimer.cancel();
            logger.info("receive signal term preCreateNextIndexTimer cancel ");
        }

        while (closedThreadCount.get() != this.workerThreads) {
            logger.error("closedThreadCount:{} workerThreads:{}", closedThreadCount.get(), this.workerThreads);
            try {
                Thread.sleep(1000l);
            } catch (InterruptedException e) {
                logger.info("Thread sleep error:{}", e);
            }
        }

        if (signal.getName().equals("TERM")) {
            this.workers.shutdown();
        }
    }

    public static void addWorkerClose() {
        closedThreadCount.addAndGet(1);
    }
}
