package com.cwbase.logback;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;

public class RedisAppender extends UnsynchronizedAppenderBase<LoggingEvent> {
	
	JedisPool pool;

	com.cwbase.logback.logstash.JSONEventLayout layout;

	// logger configurable options
	String host = "localhost";
	int port = Protocol.DEFAULT_PORT;
	String key = null;
	int timeout = Protocol.DEFAULT_TIMEOUT;
	String password = null;
	int database = Protocol.DEFAULT_DATABASE;

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
		Jedis client = pool.getResource();
		try {
			String json = layout.doLayout(event);
			client.rpush(key, json);
		} catch (Exception e) {
			e.printStackTrace();
			pool.returnBrokenResource(client);
			client = null;
		} finally {
			if (client != null) {
				pool.returnResource(client);
			}
		}
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
		super.start();
		pool = new JedisPool(new GenericObjectPool.Config(), host, port,
				timeout, password, database);
	}

	@Override
	public void stop() {
		super.stop();
		pool.destroy();
	}
}
