package com.alibaba.jstorm.daemon.supervisor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.BindException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import backtype.storm.daemon.Shutdownable;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.FileAttribute;
import com.alibaba.jstorm.utils.HttpserverUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.Pair;
import com.alibaba.jstorm.utils.TimeFormat;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class Httpserver implements Shutdownable {

	private static Logger LOG = Logger.getLogger(Httpserver.class);

	private HttpServer hs;
	private int port;
	private Map conf;

	public Httpserver(int port, Map conf) {
		this.port = port;
		this.conf = conf;
	}

	static class LogHandler implements HttpHandler {

		private String logDir;
		Map conf;

		public LogHandler(Map conf) {

			logDir = JStormUtils.getLogDir();
			this.conf = conf;

			LOG.info("logview logDir=" + logDir); // +++

		}

		public void handlFailure(HttpExchange t, String errorMsg)
				throws IOException {
			LOG.error(errorMsg);

			byte[] data = errorMsg.getBytes();
			t.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST,
					data.length);
			OutputStream os = t.getResponseBody();
			os.write(data);
			os.close();
		}

		public void handle(HttpExchange t) throws IOException {
			URI uri = t.getRequestURI();
			Map<String, String> paramMap = parseRawQuery(uri.getRawQuery());

			String cmd = paramMap
					.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD);
			if (StringUtils.isBlank(cmd) == true) {
				handlFailure(t, "Bad Request, Not set command type");
				return;
			}

			if (HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_SHOW.equals(cmd)) {
				handleShowLog(t, paramMap);
				return;
			} else if (HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_LIST
					.equals(cmd)) {
				handleListDir(t, paramMap);
				return;
			}

			handlFailure(t, "Bad Request, Not support command type " + cmd);
			return;
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

		private void handleShowLog(HttpExchange t, Map<String, String> paramMap)
				throws IOException {
			Pair<Long, byte[]> logPair = queryLog(t, paramMap);
			if (logPair == null) {
				return;
			}

			String size = String.format(
					HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_SIZE_FORMAT,
					logPair.getFirst());
			byte[] sizeByts = size.getBytes();

			byte[] logData = logPair.getSecond();

			t.sendResponseHeaders(HttpURLConnection.HTTP_OK, sizeByts.length
					+ logData.length);
			OutputStream os = t.getResponseBody();
			os.write(sizeByts);
			os.write(logData);
			os.close();
		}

		private Pair<Long, byte[]> queryLog(HttpExchange t,
				Map<String, String> paramMap) throws IOException {

			String fileParam = paramMap
					.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_LOGFILE);
			if (StringUtils.isBlank(fileParam)) {
				handlFailure(t, "Bad Request, Params Error, no log file name.");
				return null;
			}

			String logFile = Joiner.on(File.separator).join(logDir, fileParam);
			FileChannel fc = null;
			MappedByteBuffer fout = null;
			long fileSize = 0;
			byte[] ret = "Failed to get data".getBytes();
			try {
				fc = new RandomAccessFile(logFile, "r").getChannel();

				fileSize = fc.size();

				long position = fileSize
						- HttpserverUtils.HTTPSERVER_LOGVIEW_PAGESIZE;
				try {
					String posStr = paramMap
							.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_POS);
					if (StringUtils.isBlank(posStr) == false) {
						long pos = Long.valueOf(posStr);

						position = pos;
					}
				} catch (Exception e) {
					LOG.warn("Invalide position ");
				}
				if (position < 0) {
					position = 0L;
				}

				long size = Math.min(fileSize - position,
						HttpserverUtils.HTTPSERVER_LOGVIEW_PAGESIZE);

				LOG.info("logview " + logFile + ", position=" + position
						+ ", size=" + size);
				fout = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

				ret = new byte[(int) size];
				fout.get(ret);
				String str = new String(ret, ConfigExtension.getLogViewEncoding(conf));
				return new Pair<Long, byte[]>(fileSize, str.getBytes());

			} catch (FileNotFoundException e) {
				LOG.warn(e);
				handlFailure(t, "Bad Request, Failed to find " + fileParam);
				return null;

			} catch (IOException e) {
				LOG.warn(e);
				handlFailure(t, "Bad Request, Failed to open " + fileParam);
				return null;
			} finally {
				fout = null;
				if (fc != null) {
					IOUtils.closeQuietly(fc);
				}
			}

		}

		byte[] getJSonFiles(String dir) throws Exception {
			Map<String, FileAttribute> fileMap = new HashMap<String, FileAttribute>();

			
			String path = logDir;
			if (dir != null) {
				path = path + File.separator + dir;
			}
			
			LOG.info("List dir " + path);
			
			File file = new File(path);

			String[] files = file.list();

			for (String fileName : files) {
				String logFile = Joiner.on(File.separator).join(path,
						fileName);

				FileAttribute fileAttribute = new FileAttribute();
				fileAttribute.setFileName(fileName);
				
				File subFile = new File(logFile);
				
				Date modify = new Date(subFile.lastModified());
				fileAttribute.setModifyTime(TimeFormat.getSecond(modify));

				if (subFile.isFile()) {
					fileAttribute.setIsDir(String.valueOf(false));
					fileAttribute.setSize(String.valueOf(subFile.length()));
					
					
					fileMap.put(logFile, fileAttribute);
				} else if (subFile.isDirectory()) {
					fileAttribute.setIsDir(String.valueOf(true));
					fileAttribute.setSize(String.valueOf(4096));
					
					fileMap.put(logFile, fileAttribute);
				}
				
				
			}

			String fileJsonStr = JStormUtils.to_json(fileMap);
			return fileJsonStr.getBytes();
		}

		void handleListDir(HttpExchange t, Map<String, String> paramMap)
				throws IOException {
			byte[] filesJson = "Failed to get file list".getBytes();

			try {
				String dir = paramMap.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_DIR);
				filesJson = getJSonFiles(dir);
			} catch (Exception e) {
				handlFailure(t, "Failed to get file list");
				return;
			}

			t.sendResponseHeaders(HttpURLConnection.HTTP_OK, filesJson.length);
			OutputStream os = t.getResponseBody();
			os.write(filesJson);
			os.close();
		}

	}// LogHandler

	public void start() {
		int numHandler = 3;
		InetSocketAddress socketAddr = new InetSocketAddress(port);
		Executor executor = Executors.newFixedThreadPool(numHandler);

		try {
			hs = HttpServer.create(socketAddr, 0);
			hs.createContext(HttpserverUtils.HTTPSERVER_CONTEXT_PATH_LOGVIEW,
					new LogHandler(conf));
			hs.setExecutor(executor);
			hs.start();

		} catch (BindException e) {
			LOG.info("Httpserver Already start!");
			hs = null;
			return;
		} catch (IOException e) {
			LOG.error("Httpserver Start Failed", e);
			hs = null;
			return;
		}
		LOG.info("Success start httpserver at port:" + port);

	}

	@Override
	public void shutdown() {
		if (hs != null) {
			hs.stop(0);
			LOG.info("Successfully stop http server");
		}

	}

}
