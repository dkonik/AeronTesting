package com.Aeron.Testing;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;

public class Producer implements Runnable {

	private static final int MESSAGE_BYTES = 512;

	private static final int STREAMID = 10;

	int numRuns = 100000;

	public Producer(int numRuns) {
		this.numRuns = numRuns;
	}

	@Override
	public void run() {
		final MediaDriver driver = MediaDriver.launchEmbedded();

		final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())
				.idleStrategy(new BusySpinIdleStrategy());

		System.out.println("Producing to: " + App.consumerIp + " on " + STREAMID);

		try (final Aeron aeron = Aeron.connect(ctx);
				final Publication producerPublication = aeron.addPublication(App.consumerIp, STREAMID)) {

			final UnsafeBuffer atomicBuffer = new UnsafeBuffer(
					BufferUtil.allocateDirectAligned(MESSAGE_BYTES, BitUtil.CACHE_LINE_LENGTH));

			for (int i = 0; i < numRuns; ++i) {
				atomicBuffer.putLong(0, i);
				while ((producerPublication.offer(atomicBuffer)) < 0L) {
				}
				if ((i % 5) == 0) {
					final long waitTime = System.nanoTime();
					while ((System.nanoTime() - waitTime) < 20000) {
						// Busy spin
					}
				}
			}

			Thread.sleep(100);

			System.exit(0);

		} catch (final Exception e) {
			e.printStackTrace(System.out);
			throw new RuntimeException(e);
		}
	}

}
