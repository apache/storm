package com.alibaba.jstorm.ui.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class TableData implements Serializable {
    private static final long serialVersionUID = 1L;

    protected String name;
    protected List<String> headers = new ArrayList<String>();
    protected List<Map<String, ColumnData>> lines =
            new ArrayList<Map<String, ColumnData>>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<Map<String, ColumnData>> getLines() {
        return lines;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
