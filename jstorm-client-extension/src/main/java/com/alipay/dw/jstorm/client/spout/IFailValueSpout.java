package com.alipay.dw.jstorm.client.spout;

import java.util.List;


public interface IFailValueSpout{
    void fail(Object msgId, List<Object> values);
}
