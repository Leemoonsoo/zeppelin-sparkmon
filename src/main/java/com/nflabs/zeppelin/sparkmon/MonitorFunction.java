package com.nflabs.zeppelin.sparkmon;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.Function;

import com.sun.management.OperatingSystemMXBean;

public class MonitorFunction implements Function<Integer, Map<String, Double>> {

  @Override
  public Map<String, Double> call(Integer arg0) throws Exception {
    String hostname = InetAddress.getLocalHost().getHostName();

    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);
    double systemCpuLoad = osBean.getSystemCpuLoad();

    HashMap<String, Double> map = new HashMap<String, Double>();
    if (!Double.isInfinite(systemCpuLoad) && !Double.isNaN(systemCpuLoad)) {
      map.put(hostname, systemCpuLoad);
    }
    return map;
  }
}
