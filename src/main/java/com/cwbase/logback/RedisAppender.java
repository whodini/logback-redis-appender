package com.cwbase.logback;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.clients.util.SafeEncoder;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;

public class RedisAppender extends UnsynchronizedAppenderBase<LoggingEvent> implements Runnable {
	
	JedisPool pool;

	com.cwbase.logback.logstash.JSONEventLayout layout;

	// logger configurable options
	String host = "localhost";
	int port = Protocol.DEFAULT_PORT;
	String key = null;
	int timeout = Protocol.DEFAULT_TIMEOUT;
	String password = null;
	int database = Protocol.DEFAULT_DATABASE;
	private boolean daemonThread = true;
	private int batchSize = 100;
	private boolean alwaysBatch = true;
	private boolean purgeOnFailure = true;

	private Queue<LoggingEvent> events;	
	private byte[][] batch;
	private int messageIndex = 0;
	private long period = 500;
	
	private ScheduledExecutorService executor;
	private ScheduledFuture<?> task;
	
	public RedisAppender() {
		layout = new com.cwbase.logback.logstash.JSONEventLayout();
		try {
			setSourceHost(InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e) {
			setSourceHost("unknown-host");
		}
	}

	@Override
	protected void append(LoggingEvent event) {
		try {			
			populateEvent(event);
			events.add(event);
		} catch (Exception e) {
			addError("Error populating event and adding to queue", e);
		}
	}
	
	protected void populateEvent(LoggingEvent event) {
		event.getThreadName();
		event.getFormattedMessage();
		event.getMDCPropertyMap();
		event.getCallerData();
		event.getThrowableProxy();
	}

	
	public String getSourceHost() {
		return layout.getSourceHost();
	}

	public void setSourceHost(String sourceHost) {
		layout.setSourceHost(sourceHost);
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public void setMdc(boolean flag) {
		layout.setProperties(flag);
	}

	public boolean getMdc() {
		return layout.getProperties();
	}

	public void setLocation(boolean flag) {
		layout.setLocationInfo(flag);
	}

	public boolean getLocation() {
		return layout.getLocationInfo();
	}

	public void setCallerStackIndex(int index) {
		layout.setCallerStackIdx(index);
	}

	public int getCallerStackIndex() {
		return layout.getCallerStackIdx();
	}
	
	public void setUserFields(String fields){
		layout.setUserFields(fields);
	}
	
	public String getUserFields(){
		return layout.getUserFields();
	}

	@Override
	public void start() {
		try {
			super.start();

			if (key == null)
				throw new IllegalStateException("Must set 'key'");

			events = new ConcurrentLinkedQueue<LoggingEvent>();

			batch = new byte[getBatchSize()][];

			messageIndex = 0;

			if (executor == null)
				executor = Executors
						.newSingleThreadScheduledExecutor(new NamedThreadFactory(
								"RedisAppender", daemonThread));

			if (task != null && !task.isDone())
				task.cancel(true);

			pool = new JedisPool(new GenericObjectPool.Config(), host, port,
					timeout, password, database);
			task = executor.scheduleWithFixedDelay(this, period, period,
					TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			addError("Error during start of appender.", e);
		}
	}

	@Override
	public void stop() {
		try {
			super.stop();
			task.cancel(false);
			executor.shutdown();
			pool.destroy();
		} catch (Exception e) {
			addError("Error stopping redis appender", e);
		}
	}

	@Override
	public void run() {
		try {
			if (messageIndex == getBatchSize()) push();

			LoggingEvent event;
			while ((event = events.poll()) != null) {
				try {
					String message = layout.doLayout(event);
					batch[messageIndex++] = SafeEncoder.encode(message);
				} catch (Exception e) {
					addError(e.getMessage(), e);
				}

				if (messageIndex == getBatchSize()) push();
			}

			if (!getAlwaysBatch() && messageIndex > 0) push();
		} catch (Exception e) {
			addError(e.getMessage(), e);
		}
	}
	
	private void push() {		
		Jedis client = null;
		try {
			client = pool.getResource();

			client.rpush(
					SafeEncoder.encode(key),
					getBatchSize() == messageIndex ? batch : Arrays.copyOf(batch,
							messageIndex));
			messageIndex = 0;

		} catch (Exception e) {
			addError("Error occurred while pushing messages to redis", e);
			pool.returnBrokenResource(client);
			client = null;

			if (getPurgeOnFailure()) {
				addInfo("Purging event queue");
				events.clear();
				messageIndex = 0;
			}
		} finally {
			if (client != null) {
				pool.returnResource(client);
			}
		}
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public boolean getAlwaysBatch() {
		return alwaysBatch;
	}

	public void setAlwaysBatch(boolean alwaysBatch) {
		this.alwaysBatch = alwaysBatch;
	}

	public boolean getPurgeOnFailure() {
		return purgeOnFailure;
	}

	public void setPurgeOnFailure(boolean purgeOnFailure) {
		this.purgeOnFailure = purgeOnFailure;
	}
	
}
