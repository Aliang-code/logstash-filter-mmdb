package org.logstashplugins;

import java.util.HashMap;
import java.util.Map;

public class FieldNode {
    private String name;
    private String target;
    private Map<String, FieldNode> childMap;

    public FieldNode(String name, String target) {
        this.name = name;
        this.target = target;
        this.childMap=new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public Map<String, FieldNode> getChildMap() {
        return childMap;
    }

    public void setChildMap(Map<String, FieldNode> childMap) {
        this.childMap = childMap;
    }
}
