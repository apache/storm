package backtype.storm.utils;

import java.io.Serializable;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ServerInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private String host;
  private int port;

  public static ServerInfo instance(String jsonText) throws ParseException {
    JSONParser parser = new JSONParser();
    JSONObject jsonObject = (JSONObject) parser.parse(jsonText);
    String host = (String) jsonObject.get("host");
    Long port = (Long) jsonObject.get("port");
    return new ServerInfo(host, port.intValue());
  }

  public ServerInfo(String host, int port) {
    this.host = host;
    this.port = port;
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

  public String toString() {
    return host + ":" + port;
  }

  public String toJsonString() {
    JSONObject obj = new JSONObject();
    obj.put("host", host);
    obj.put("port", new Integer(port));
    return obj.toJSONString();
  }
}
