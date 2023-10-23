package io.debezium.performance.testsuite.dmt;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.performance.dmt.schema.LoadResult;
import io.debezium.performance.testsuite.exception.DmtException;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.debezium.performance.testsuite.ConfigProperties.DMT_URL;

public class BareDmtController implements DmtController {
    private final OkHttpClient client = new OkHttpClient();
    private final Logger LOG = LoggerFactory.getLogger(BareDmtController.class);

    public BareDmtController() {}

    @Override
    public LoadResult generateSqlBatchLoad(int count, int maxRows) {
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("count", String.valueOf(count));
        queryParameters.put("maxRows", String.valueOf(maxRows));
        return useCustomPostEndpoint("GenerateBatchLoad", queryParameters);
    }

    @Override
    public LoadResult generateMongoBulkLoad(int count, int maxRows, int messageSize) {
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("count", String.valueOf(count));
        queryParameters.put("maxRows", String.valueOf(maxRows));
        queryParameters.put("messageSize", String.valueOf(messageSize));
        return useCustomPostEndpoint("GenerateMongoBulkSizedLoad", queryParameters);
    }

    @Override
    public LoadResult useCustomPostEndpoint(String name, Map<String, String> queryParameters) {
        if (DMT_URL == null) {
            LOG.error("DMT URL not specified. Cannot send request");
            throw new DmtException("DMT URL not specified. Cannot send request");
        }
        HttpUrl.Builder httpBuilder = Objects.requireNonNull(HttpUrl.parse(DMT_URL + "/" + name)).newBuilder();
        for(Map.Entry<String, String> param : queryParameters.entrySet()) {
            httpBuilder.addQueryParameter(param.getKey(), param.getValue());
        }
        @SuppressWarnings("KotlinInternalInJava") Request request = new Request.Builder()
                .url(httpBuilder.build())
                .post(RequestBody.create("", null))
                .build();
        return executeRequest(request);
    }

    private LoadResult executeRequest(Request request) {
        try (Response response = client.newCall(request).execute()) {
            if (response.code() != 200) {
                LOG.error(String.format("DMT endpoint %s error, the response code was not 200", request.url()));
                throw new DmtException(String.format("DMT endpoint %s error, the response code was not 200", request.url()));
            }
            if (response.body() == null) {
                LOG.error(String.format("DMT endpoint %s error, the response body is missing", request.url()));
                return new LoadResult(-1, -1, -1);
            }
            return new ObjectMapper().readValue(response.body().string(), LoadResult.class);
        } catch (IOException ex) {
            LOG.error(String.format("DMT endpoint %s could not send request", request.url()));
            throw new DmtException(String.format("DMT endpoint %s could not send request", request.url()), ex.getCause());
        }
    }
}
