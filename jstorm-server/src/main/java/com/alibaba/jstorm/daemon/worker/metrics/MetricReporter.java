package com.alibaba.jstorm.daemon.worker.metrics;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class MetricReporter  {

	final Slf4jReporter reporter1Minute;
	final Slf4jReporter reporter10Minute;
	
	private boolean isEnable;

	public MetricReporter() {
		reporter1Minute = Slf4jReporter.forRegistry(Metrics.getMetrics())
				.outputTo(LoggerFactory.getLogger(MetricReporter.class))
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS).build();
		
		
		reporter10Minute = Slf4jReporter.forRegistry(Metrics.getJstack())
				.outputTo(LoggerFactory.getLogger(MetricReporter.class))
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS).build();
		
	}
	
	public void start() {
		reporter1Minute.start(1, TimeUnit.MINUTES);
		reporter10Minute.start(10, TimeUnit.MINUTES);
		
	}
	
	public void shutdown() {
		reporter10Minute.close();
		reporter1Minute.close();
	}
	
	

	public boolean isEnable() {
		return isEnable;
	}

	public void setEnable(boolean isEnable) {
		this.isEnable = isEnable;
		JStormTimer.setEnable(isEnable);
		JStormHistogram.setEnable(isEnable);
	}



	private static class LatencyRatio implements Gauge<Double> {
		Timer timer;

		protected LatencyRatio(Timer base) {
			timer = base;
		}

		@Override
		public Double getValue() {
			Snapshot snapshot = timer.getSnapshot();
			return snapshot.getMedian() / 1000000;
		}

	}

	public static void main(String[] args) {
		final Random random = new Random();
		random.setSeed(System.currentTimeMillis());

		Thread thread = new Thread(new Runnable() {

			final JStormTimer timer = Metrics.registerTimer("timer");
			final Meter meter = Metrics.registerMeter("meter");
			LatencyRatio latency = Metrics.getMetrics().register("latency", new LatencyRatio(timer.getInstance()));

			@Override
			public void run() {
				System.out.println("Begin to run");
				int counter = 0;
				while (counter++ < 40000) {
					meter.mark();
					timer.start();

					int rand = random.nextInt(10);

					try {
						Thread.sleep(rand * 1);
					} catch (InterruptedException e) {

					} finally {
						timer.stop();
					}

					try {
						Thread.sleep(2);
					} catch (InterruptedException e) {
					}
					if (counter % 1000 == 0) {
						System.out.println("Done " + counter);
					}

				}
			}
		});

		Metrics.getMetrics().registerAll(Metrics.getJstack());
		final ConsoleReporter reporter = ConsoleReporter.forRegistry(Metrics.getMetrics())
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS).build();
		reporter.start(1, TimeUnit.MINUTES);

		thread.start();
	}
}
