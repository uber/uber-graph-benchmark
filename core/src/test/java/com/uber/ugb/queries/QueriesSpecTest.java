package com.uber.ugb.queries;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class QueriesSpecTest {
    @Test
    public void testLoadingQueriesSpec() throws IOException, URISyntaxException {

        Path path = Paths.get(QueriesSpec.class.getResource("/specs/queries.yaml").toURI().getPath());
        InputStream yamlInput = new FileInputStream(new File(path.toString()));

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        QueriesSpec queriesSpec = objectMapper.readValue(yamlInput, QueriesSpec.class);

        Assert.assertEquals(1, queriesSpec.queries.length);

        QueriesSpec.Query query = queriesSpec.queries[0];
        Assert.assertEquals("client.trip.driver.payment_profile.payment_instrument", query.name);
        Assert.assertEquals("k-hop", query.type);
        Assert.assertEquals("User", query.startVertexLabel);
        Assert.assertEquals(4, query.steps.length);

        Assert.assertEquals("requested", query.steps[0].edge.label);
        Assert.assertEquals(10, query.steps[0].edge.limit);
        Assert.assertEquals("tripAmount", query.steps[0].vertex.select);

        Assert.assertEquals("in", query.steps[1].edge.direction);

    }
}
