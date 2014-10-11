package com.alibaba.jstorm.yarn;

public class Version {

	private final String version;
	  private final String build;
	  
	  public Version(String version, String build) {
	    this.version = version;
	    this.build = build;
	  }
	  
	  public String version() {
	    return this.version;
	  }

	  public String build() {
	    return this.build;
	  }
	  
	  @Override
	  public String toString() {
	    if (null == build || build.isEmpty()) {
	      return version;
	    } else {
	      return version + "-" + build;
	    }
	  }
}
