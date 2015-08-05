package com.alibaba.jstorm.ui.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.jstorm.utils.JStormUtils;

public class ColumnData implements Serializable {
    List<String> texts = new ArrayList<String>();
    List<LinkData> links = new ArrayList<LinkData>();

    public List<String> getTexts() {
        return texts;
    }

    public void setTexts(List<String> texts) {
        this.texts = texts;
    }

    public List<LinkData> getLinks() {
        return links;
    }

    public void setLinks(List<LinkData> links) {
        this.links = links;
    }

    public void addText(String text) {
        try {
            Double value = Double.valueOf(text);
            
            if(Math.abs(value - Math.round(value)) < 0.001){
                texts.add(text);
            }else {
                texts.add(JStormUtils.formatSimpleDouble(value));
            }
            
            
        }catch(Exception e) {
            texts.add(text);
        }
        
    }

    public void addLinkData(LinkData linkData) {
        links.add(linkData);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
