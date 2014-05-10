package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

public class LogPageIndex  implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -6305581906533640556L;
    
    
    private String index;
    private String pos;
    public String getIndex() {
        return index;
    }
    public void setIndex(String index) {
        this.index = index;
    }
    public String getPos() {
        return pos;
    }
    public void setPos(String pos) {
        this.pos = pos;
    }
    
    
}
