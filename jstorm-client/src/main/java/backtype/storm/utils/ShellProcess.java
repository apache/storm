package backtype.storm.utils;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.cli.shell.CmdShell;

import backtype.storm.task.TopologyContext;


public class ShellProcess {
	public static Logger LOG = Logger.getLogger(ShellProcess.class);
	private DataOutputStream processIn;
	private BufferedReader processOut;
	private InputStream processErrorStream;
	private Process _subprocess;
	private String[] command;

	public ShellProcess(String[] command) {
		this.command = command;
	}

	public Number launch(Map conf, TopologyContext context) throws IOException {
		ProcessBuilder builder = new ProcessBuilder(command);
		builder.directory(new File(context.getCodeDir()));
		_subprocess = builder.start();

		processIn = new DataOutputStream(_subprocess.getOutputStream());
		processOut = new BufferedReader(new InputStreamReader(
				_subprocess.getInputStream()));
		processErrorStream = _subprocess.getErrorStream();

		Map setupInfo = new HashMap();
		setupInfo.put("pidDir", context.getPIDDir());
		setupInfo.put("conf", conf);
		setupInfo.put("context", context);
		writeMessage(setupInfo);
		
		StringBuffer sb = new StringBuffer();
		sb.append("Begin to run command:");
		for (String cmd: command) {
			sb.append(cmd);
		}
		sb.append(setupInfo);
		LOG.info(sb.toString());

		return (Number) readMessage().get("pid");
	}

	public void destroy() {
		_subprocess.destroy();
	}

	public void writeMessage(Object msg) throws IOException {
		writeString(Utils.to_json(msg));
	}

	private void writeString(String str) throws IOException {
		byte[] strBytes = str.getBytes("UTF-8");
		processIn.write(strBytes, 0, strBytes.length);
		processIn.writeBytes("\nend\n");
		processIn.flush();
	}

	public Map readMessage() throws IOException {
		String string = readString();
		Map msg =  (Map)Utils.from_json(string);
		if (msg != null) {
			return msg;
		} else {
			throw new IOException("unable to parse: " + string);
		}
	}

	public String getErrorsString() {
		if (processErrorStream != null) {
			try {
				return IOUtils.toString(processErrorStream);
			} catch (IOException e) {
				return "(Unable to capture error stream)";
			}
		} else {
			return "";
		}
	}

	public void drainErrorStream() {
		try {
			while (processErrorStream.available() > 0) {
				int bufferSize = processErrorStream.available();
				byte[] errorReadingBuffer = new byte[bufferSize];

				processErrorStream.read(errorReadingBuffer, 0, bufferSize);

				LOG.info("Got error from shell process: "
						+ new String(errorReadingBuffer));
			}
		} catch (Exception e) {
		}
	}

	private String readString() throws IOException {
		StringBuilder line = new StringBuilder();

		// synchronized (processOut) {
		while (true) {
			String subline = processOut.readLine();
			if (subline == null) {
				StringBuilder errorMessage = new StringBuilder();
				errorMessage.append("Pipe to subprocess seems to be broken!");
				if (line.length() == 0) {
					errorMessage.append(" No output read.\n");
				} else {
					errorMessage.append(" Currently read output: "
							+ line.toString() + "\n");
				}
				errorMessage.append("Shell Process Exception:\n");
				errorMessage.append(getErrorsString() + "\n");
				throw new RuntimeException(errorMessage.toString());
			}
			if (subline.equals("end")) {
				break;
			}
			if (line.length() != 0) {
				line.append("\n");
			}
			line.append(subline);
		}
		// }

		return line.toString();
	}
}
