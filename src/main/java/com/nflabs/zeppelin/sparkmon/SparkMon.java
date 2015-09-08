package com.nflabs.zeppelin.sparkmon;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.Pool;
import org.apache.zeppelin.helium.Application;
import org.apache.zeppelin.helium.ApplicationArgument;
import org.apache.zeppelin.helium.ApplicationException;
import org.apache.zeppelin.helium.Signal;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.dev.ZeppelinApplicationDevServer;
import org.apache.zeppelin.resource.ResourceKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Enumeration.Value;

/**
 * Monitor Spark cluster reousrces
 */
public class SparkMon extends Application implements Runnable {
  Logger logger = LoggerFactory.getLogger(SparkMon.class);
  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> monitorHandle;
  private InterpreterContext context;
  private SparkContext sc;
  private JavaSparkContext jsc;
  private HashMap<String, Integer> cpuInfo;
  public static int DEFAULT_INTERVAL_SEC = 5;


  protected void onChange(String name, Object oldObject, Object newObject) {
    if ("interval".equals(name)) {
      int interval = Integer.parseInt((String) newObject);
      logger.info("Interval changed to {}", interval);

      this.stopMonitor();
      if (interval > 0) {
        this.startMonitor(interval);
      } else {
        resetNumber();
      }
    }
  }

  public void signal(Signal signal) {
  }

  public void load() throws ApplicationException, IOException {
    cpuInfo = new HashMap<String, Integer>();
  }


  public void run(ApplicationArgument arg, InterpreterContext context) throws ApplicationException,
      IOException {
    // load resource from classpath
    context.out.writeResource("spark-mon/SparkMon.html");

    // get spark context
    sc = (SparkContext) context.getResourcePool().get(
        arg.getResource().location(), arg.getResource().name());

    if (sc == null) {
      throw new ApplicationException("SparkContext not available");
    }

    jsc = new JavaSparkContext(sc);

    // add jar
    URLClassLoader cl = (URLClassLoader) Thread.currentThread().getContextClassLoader();
    for (URL url : cl.getURLs()) {
      if (url.getPath().matches(".*spark-mon-[0-9.]*.*jar")) {
        sc.addJar(url.getPath());
        break;
      }
    }

    this.context = context;

    // put default Interval
    put(context, "interval", Integer.toString(DEFAULT_INTERVAL_SEC));

    // watch interval change
    watch(context, "interval");

    startMonitor(DEFAULT_INTERVAL_SEC);

  }

  @Override
  public void run() {
    // set scheduler pool
    sc.setLocalProperty("spark.scheduler.pool", "fair");

    // create task
    int numSlave = sc.getExecutorStorageStatus().length;
    numSlave = Math.max(numSlave, 4);

    List<Integer> list = new LinkedList<Integer>();
    for (int i = 0; i < numSlave; i++) {
      list.add(new Integer(i));
    }
    JavaRDD<Integer> rdd = jsc.parallelize(list, numSlave);

    List<Map<String, Double>> cpuLoadMap = rdd.map(new MonitorFunction()).collect();

    setCpuLoad(cpuLoadMap);
  }

  private synchronized void setCpuLoad(List<Map<String, Double>> cpuLoadMap) {
    for (Map<String, Double> cpu : cpuLoadMap) {
      for (String hostname : cpu.keySet()) {
        Integer oldValue = cpuInfo.get(hostname);
        if (oldValue == null) {
          oldValue = 0;
        }
        Integer newValue = ((Double) (cpu.get(hostname) * 100)).intValue();
        cpuInfo.put(hostname, (int)((oldValue * 0.7) + (newValue * 0.3)));
      }
    }

    this.put(context, "cpuInfo", cpuInfo);
  }


  public void startMonitor(int intervalSec) {
    if (monitorHandle != null) {
      return;
    }
    scheduler = Executors.newScheduledThreadPool(1);
    monitorHandle = scheduler.scheduleAtFixedRate(this, 1, intervalSec, TimeUnit.SECONDS);
  }

  public void stopMonitor() {
    if (monitorHandle != null) {
      monitorHandle.cancel(true);
      monitorHandle = null;
    }
  }

  private void resetNumber() {
    for (String hostname : cpuInfo.keySet()) {
      cpuInfo.put(hostname, 0);
    }
    this.put(context, "cpuInfo", cpuInfo);
  }


  public void unload() throws ApplicationException, IOException {
    stopMonitor();
  }


  /**
   * Development mode
   * @param args
   * @throws Exception
   */
  public static void main(String [] args) throws Exception {
    // create development server
    ZeppelinApplicationDevServer dev = new ZeppelinApplicationDevServer(SparkMon.class.getName());

    // create spark conf
    SparkConf conf = new SparkConf();
    conf.setMaster("local[*]");
    conf.setAppName("SparkMon");

    // create spark context
    SparkContext sc = new SparkContext(conf);

    // create fair scheduler pool. as SparkInterpreter do
    if (sc.getPoolForName("fair").isEmpty()) {
      Value schedulingMode = org.apache.spark.scheduler.SchedulingMode.FAIR();
      int minimumShare = 0;
      int weight = 1;
      Pool pool = new Pool("fair", schedulingMode, minimumShare, weight);
      sc.taskScheduler().rootPool().addSchedulable(pool);
    }

    dev.server.getResourcePool().put("sparkcontext", sc);

    // set application argument
    ApplicationArgument arg = new ApplicationArgument(new ResourceKey(
        dev.server.getResourcePoolId(),
        "sparkcontext"
        ));
    dev.setArgument(arg);

    // start
    dev.server.start();
    dev.server.join();
  }


}
