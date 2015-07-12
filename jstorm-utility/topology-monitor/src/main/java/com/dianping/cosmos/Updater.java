package com.dianping.cosmos;

import java.io.Serializable;

public interface Updater extends Serializable {

    byte[] update(byte[] oldValue, byte[] newValue);

}
