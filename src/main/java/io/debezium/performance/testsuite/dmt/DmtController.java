package io.debezium.performance.testsuite.dmt;

import io.debezium.performance.dmt.schema.LoadResult;
import java.util.Map;

public interface DmtController {
    LoadResult generateSqlBatchLoad(int count, int maxRows);

    LoadResult generateSizedSqlBatchLoad(int count, int maxRows, int messageSize);
    LoadResult generateMongoBulkLoad(int count, int maxRows, int messageSize);
    LoadResult useCustomPostEndpoint(String name, Map<String, String> queryParameters);
}
