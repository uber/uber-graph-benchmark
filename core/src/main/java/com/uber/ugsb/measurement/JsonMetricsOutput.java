package com.uber.ugsb.measurement;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class JsonMetricsOutput implements MetricsOutput {

    JsonObject json;

    public JsonMetricsOutput() {
        json = new JsonObject();
    }

    @Override
    public void write(String category, String metric, long i) {
        JsonObject categoryObject = json.getAsJsonObject(category);
        if (categoryObject == null) {
            categoryObject = new JsonObject();
            json.add(category, categoryObject);
        }
        categoryObject.add(metric, new JsonPrimitive(i));
    }

    @Override
    public void write(String category, String metric, double d) {
        JsonObject categoryObject = json.getAsJsonObject(category);
        if (categoryObject == null) {
            categoryObject = new JsonObject();
            json.add(category, categoryObject);
        }
        categoryObject.add(metric, new JsonPrimitive(d));
    }

    public JsonObject getJson() {
        return json;
    }
}
