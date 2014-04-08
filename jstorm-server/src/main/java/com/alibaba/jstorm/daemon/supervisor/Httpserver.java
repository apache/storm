package com.alibaba.jstorm.daemon.supervisor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

import backtype.storm.daemon.Shutdownable;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.worker.Worker;
import com.alibaba.jstorm.utils.HttpserverUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class Httpserver implements Shutdownable {
	
	private static Logger LOG = Logger.getLogger(Httpserver.class); 
	
	private static Map storm_conf;
	private HttpServer hs;
	
	public Httpserver (Map storm_conf) {
		this.storm_conf = storm_conf;
	}
	 
	static class LogHandler implements HttpHandler {
		
		enum ViewMode {
			l4k, l8k, all;
		}
		
		private String logDir;
		
		public LogHandler() {
			Enumeration<Appender> enumAppender = Logger.getRootLogger().getAllAppenders();
			FileAppender fileAppender = null;
			while (enumAppender.hasMoreElements()) {
				Appender appender = enumAppender.nextElement();
				if (appender instanceof FileAppender) {
					fileAppender = (FileAppender)appender;
					break;
				}
			}
			if (fileAppender != null) {
				String file = fileAppender.getFile();
				logDir = file.substring(0, file.lastIndexOf('/'));
			} 
			
			LOG.info("logview logDir=" + logDir);  //+++
			
		}
		
		public void handle(HttpExchange t) throws IOException {
			URI uri = t.getRequestURI();
			Map<String, String> paramMap = parseRawQuery(uri.getRawQuery());
			byte[] logBytes = queryLog(paramMap);
//		    InputStream is = t.getRequestBody();  
//		    byte[] temp = new byte[is.available()];  
//		    is.read(temp);
		    t.sendResponseHeaders(200, logBytes.length);
		    OutputStream os = t.getResponseBody();  
		    os.write(logBytes);  
		    os.close();  
		}
		
		private Map<String, String> parseRawQuery(String uriRawQuery) {
			Map<String, String> paramMap = Maps.newHashMap();
			
			for (String param : StringUtils.split(uriRawQuery, "&")) {
				String[] pair = StringUtils.split(param, "=");
				if (pair.length == 2) {
					paramMap.put(pair[0], pair[1]);
				}
			}
			
			return paramMap;
		}
		
		private byte[] queryLog(Map<String, String> paramMap) {
			String fileParam = paramMap.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_LOGFILE);
			if (StringUtils.isEmpty(fileParam)) {
				return "Bad Request, Params Error".getBytes();
			}
			
			ViewMode viewMode = ViewMode.l4k;
			String modeParam = paramMap.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_VIEWMODE);
			if (StringUtils.isNotEmpty(modeParam)) {
				try {
					viewMode = ViewMode.valueOf(modeParam);
				} catch (Throwable e) {
					LOG.warn("Not Support Logview Mode : " + modeParam + ", Display Last 4KB!");
				}
			}
			
			String logFile = Joiner.on("/").join(logDir, fileParam);
			FileChannel fc = null;
			MappedByteBuffer fout = null;
			byte[] ret;
	        try {
	        	fc = new RandomAccessFile(logFile, "r").getChannel();
	        	
	        	long size = 0;
	        	long position = -1;
	        	switch (viewMode) {
	        		case l4k :
	        			size = 1024 * 4;
	        			position = fc.size() - size;
	        			break;
	        		case l8k :
	        			size = 1024 * 8;
	        			position = fc.size() - size;
	        			break;
	        		case all :
	        			size = fc.size();
	        			position = 0L;
	        	}
	        	if (position < 0) {
	        		position = 0L;
	        	}
	        	
	        	LOG.info("logview " + logFile + ", position=" + position + ", size=" + size);
	        	fout = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
	        	
	        	ret = new byte[(int)size];
	        	fout.get(ret);
	        	return ret;
	        	
	        } catch (FileNotFoundException e) {
	        	LOG.warn(e);
	        	return e.toString().getBytes();
	        	
	        } catch (IOException e) {
	        	LOG.warn(e);
	        	return e.toString().getBytes();
	        	
	        } finally {
	        	fout = null;
	        	if (fc != null) {
	        		IOUtils.closeQuietly(fc);
	        	}
	        }
		}
	}
	

	public void start() {
		int numHandler = 3;
		int port = ConfigExtension.getSupervisorHttpserverPort(storm_conf);
		InetSocketAddress socketAddr = new InetSocketAddress(port);
		Executor executor = Executors.newFixedThreadPool(numHandler);
		
		try {  
			hs = HttpServer.create(socketAddr, 0);  
	        hs.createContext(HttpserverUtils.HTTPSERVER_CONTEXT_PATH_LOGVIEW, new LogHandler());  
	        hs.setExecutor(executor);
	        hs.start();
	        
        } catch (IOException e) {  
            LOG.error("Httpserver Start Failed", e);  
        } 
		LOG.info("Success start supervisor httpserver at port:" + port);
		
	}
	

	@Override
	public void shutdown() {
		if (hs != null) {
			hs.stop(0);
		}
		
	}
	
}
