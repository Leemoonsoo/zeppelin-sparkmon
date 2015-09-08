package com.nflabs.zeppelin.sparkmon;

import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;

import org.junit.Test;

import com.sun.management.OperatingSystemMXBean;

public class SparkMonTest {



  @Test
  public void testUsingMXBean() {
  OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
      OperatingSystemMXBean.class);
    //What % CPU load this current JVM is taking, from 0.0-1.0
    System.out.println(osBean.getProcessCpuLoad());

    //What % load the overall system is at, from 0.0-1.0
    System.out.println(osBean.getSystemCpuLoad());
  }

}
