package com.Aeron.Testing;

/**
 * Hello world!
 *
 */
public class App {

  // Change this to change number of Runs
  public static int numRuns = 10000000;

  public static String consumerIp;

  /**
   * Args:<br>
   * <br>
   * The UDP endpoint to use, arg[0] = "udp://x.x.x.x:<port>" <br>
   * To run as the consumer, arg[1] = "-c"
   * 
   */
  public static void main(String[] args) {
    boolean isConsumer;
    consumerIp = args[0];
    if (args[1].equals("-c")) {
      isConsumer = true;
    }
    else {
      isConsumer = false;

    }

    final Thread consumerThread = new Thread(new Consumer(numRuns));
    final Thread producerThread = new Thread(new Producer(numRuns));

    if (isConsumer) {
      System.out.println("CONSUMING...");
      consumerThread.start();
    }
    else {
      System.out.println("PRODUCING...");
      producerThread.start();
    }

  }
}
