package com.uber.ugsb.db;

import javax.script.ScriptException;

public class QueryCapability {
    public interface SupportGremlin {
        Object queryByGremlin(String gremlinQuery, Object... bindVariables) throws ScriptException;
    }
}
