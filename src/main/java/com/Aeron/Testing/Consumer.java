package com.Aeron.Testing;

import java.util.concurrent.CountDownLatch;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

public class Consumer implements Runnable {
  private static final int STREAMID = 10;

  public static final int FRAGMENT_COUNT_LIMIT = 256;

  private static final CountDownLatch PONG_IMAGE_LATCH = new CountDownLatch(1);

  int messageNum = 0;

  int numMessages = 100000;

  long previous = -1;

  public Consumer(int numMessages) {
    this.numMessages = numMessages;
  }

  @Override
  public void run() {
    final MediaDriver driver = MediaDriver.launchEmbedded();

    final IdleStrategy BUSY_SPIN_IDLE_STRATEGY = new BusySpinIdleStrategy();

    // Handles the messages
    final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
      // We observe this this call to getLong() occasionally returns an
      // incorrect value
      final long receivedIndex = buffer.getLong(offset);

      if (receivedIndex != previous + 1) {
        System.out.println("MISSED MESSAGE AT INDEX: " + previous + "->" + receivedIndex);
        System.out.printf("%x %x %x %x %x %x %x %x\n", buffer.getByte(offset), buffer.getByte(offset + 1),
            buffer.getByte(offset + 2), buffer.getByte(offset + 3), buffer.getByte(offset + 4),
            buffer.getByte(offset + 5), buffer.getByte(offset + 6), buffer.getByte(offset + 7),
            buffer.getByte(offset + 8));
        // We anticipate that this call to getLong() will now return the correct
        // value
        System.out.println("Offset(again): " + buffer.getLong(offset) + "\n");
      }

      ++messageNum;
      previous = receivedIndex;
    };

    // Start aeron instance on Consumer side
    final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())
        .idleStrategy(new BusySpinIdleStrategy())
        .availableImageHandler(Consumer::availableConsumerImageHandler);

    System.out.println("Listening on: " + App.consumerIp + " on " + STREAMID);

    try (final Aeron aeron = Aeron.connect(ctx);
        final Subscription producerSubscription = aeron.addSubscription(App.consumerIp, STREAMID)) {
      BUSY_SPIN_IDLE_STRATEGY.reset();

      while (this.messageNum < numMessages) {
        BUSY_SPIN_IDLE_STRATEGY.idle(producerSubscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT));

      }
    }
    catch (final Exception e) {
      throw new RuntimeException(e);
    }

    try {
      Thread.sleep(1000);
    }
    catch (final InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    System.exit(0);
  }

  private static void availableConsumerImageHandler(final Image image) {
    final Subscription subscription = image.subscription();
    if (STREAMID == subscription.streamId() && App.consumerIp.equals(subscription.channel())) {
      PONG_IMAGE_LATCH.countDown();
    }
  }
}
