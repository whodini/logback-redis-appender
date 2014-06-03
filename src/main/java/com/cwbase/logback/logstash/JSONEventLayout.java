/**
 * 
 */
package com.cwbase.logback.logstash;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import net.minidev.json.JSONObject;

import org.apache.commons.lang.time.FastDateFormat;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.LayoutBase;

/**
 * Creates JSON event layout supported by logstash 1.2 and above.
 * @author nitin
 *
 */
public class JSONEventLayout extends LayoutBase<ILoggingEvent> {	
	private static Integer version = 1;
	
	private JSONObject logstashEvent;
	private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
	private static final FastDateFormat ISO_DATETIME_TIME_ZONE_FORMAT_WITH_MILLIS = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", UTC);
	
	private String sourceHost;	
	private long timestamp;
	private HashMap<String, Object> exceptionInformation;
	private boolean locationInfo = false;
	private int callerStackIdx = 0;
	private boolean properties = false;

	private Map<String, String> mdc;
	private String userFields;	
	
	@Override
	public synchronized String doLayout(ILoggingEvent event) {
		logstashEvent = new JSONObject();
		exceptionInformation = new HashMap<String, Object>();
		timestamp = event.getTimeStamp();
		
		logstashEvent.put("@version", version);
        logstashEvent.put("@timestamp", dateFormat(timestamp));
        
        /**
         * Extract and add fields from log4j config, if defined
         */
        if (getUserFields() != null) {
            String userFlds = getUserFields();           
            addUserFields(userFlds);
        }   
        
        
        /**
         * Appender will initialize this value to the default host if not overridden by configuration.
         */
        logstashEvent.put("source_host", getSourceHost());
        
        logstashEvent.put("message", event.getFormattedMessage());
        
        //Construct the exception information node.        
		if (event.getThrowableProxy() != null) {
			IThrowableProxy throwableProxy = event.getThrowableProxy();

			if (throwableProxy.getClassName() != null)
				exceptionInformation.put("exception_class",
						throwableProxy.getClassName());

			exceptionInformation.put("exception_message",
					throwableProxy.getMessage());
			exceptionInformation.put("stacktrace",
					ThrowableProxyUtil.asString(throwableProxy));

			addEventData("exception", exceptionInformation);
		}
		
		if (getLocationInfo()) {
			StackTraceElement[] callerDataArray = event.getCallerData();
			if (callerDataArray != null
					&& callerDataArray.length > callerStackIdx) {

				StackTraceElement immediateCallerData = callerDataArray[callerStackIdx];
				addEventData("class", immediateCallerData.getClassName());
				addEventData("method", immediateCallerData.getMethodName());
				addEventData("file", immediateCallerData.getFileName());
				addEventData("line_number",
						Integer.toString(immediateCallerData.getLineNumber()));
			}
		}          
		
		if(properties){
			mdc = event.getMDCPropertyMap();			
			addEventData("mdc", mdc);		
		}
       
		addEventData("logger_name", event.getLoggerName());
		addEventData("level", event.getLevel().toString());
        addEventData("thread_name", event.getThreadName());
		
        return logstashEvent.toString() + "\n";
	}

	public static String dateFormat(long timestamp) {
        return ISO_DATETIME_TIME_ZONE_FORMAT_WITH_MILLIS.format(timestamp);
    }
	
	private void addEventData(String keyname, Object keyval) {
		if (null != keyval) {
			logstashEvent.put(keyname, keyval);
		}
	}

	public String getSourceHost() {
		return sourceHost;
	}

	public void setSourceHost(String sourceHost) {
		this.sourceHost = sourceHost;
	}
	
	/**
	 * Query whether log messages include location information.
	 * 
	 * @return true if location information is included in log messages, false
	 *         otherwise.
	 */
	public boolean getLocationInfo() {
		return locationInfo;
	}
	
	/**
	 * Set whether log messages should include location information.
	 * 
	 * @param locationInfo true if location information should be included, false otherwise.
	 */
	public void setLocationInfo(boolean locationInfo) {
		this.locationInfo = locationInfo;
	}
	
	public int getCallerStackIdx() {
		return callerStackIdx;
	}

	/**
	 * Location information dump with respect to call stack level. Some
	 * framework (Play) wraps the original logging method, and dumping the
	 * location always log the file of the wrapper instead of the actual caller.
	 * For PlayFramework, I use 2.
	 * 
	 * @param callerStackIdx
	 */
	public void setCallerStackIdx(int callerStackIdx) {
		this.callerStackIdx = callerStackIdx;
	}

	public boolean getProperties() {
		return properties;
	}

	public void setProperties(boolean properties) {
		this.properties = properties;
	}

	public String getUserFields() {
		return userFields;
	}

	public void setUserFields(String userFields) {
		this.userFields = userFields;
	}
	
	private void addUserFields(String data) {
		if (null != data) {
			String[] pairs = data.split(",");
			for (String pair : pairs) {
				String[] userField = pair.split(":", 2);
				if (userField[0] != null) {
					String key = userField[0];
					String val = userField[1];
					addEventData(key, val);
				}
			}
		}
	}
}
