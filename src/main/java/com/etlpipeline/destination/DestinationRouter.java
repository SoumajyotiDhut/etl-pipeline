package com.etlpipeline.destination;

import com.etlpipeline.model.DataRecord;
import com.etlpipeline.model.WriteResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class DestinationRouter {

    private final DatabaseDestination databaseDestination;
    private final FileDestination     fileDestination;

    /**
     * Route records to the correct destination
     * based on the "type" field in destination config.
     *
     * @param records     Transformed records to write
     * @param destination Destination config from pipeline definition
     */
    @SuppressWarnings("unchecked")
    public WriteResult route(List<DataRecord> records,
                             Map<String, Object> destination) {
        if (destination == null) {
            return WriteResult.failed("Destination config is required");
        }

        String type = (String) destination.get("type");
        Map<String, Object> config =
                (Map<String, Object>) destination.get("config");

        if (type == null) {
            return WriteResult.failed("Destination type is required");
        }
        if (config == null) {
            return WriteResult.failed(
                    "Destination config is required for type: " + type);
        }

        log.info("Routing to destination type: {}", type);

        switch (type.toLowerCase()) {
            case "database":
                return databaseDestination.write(records, config);
            case "file":
                return fileDestination.write(records, config);
            default:
                return WriteResult.failed(
                        "Unknown destination type: " + type
                                + ". Supported: database, file");
        }
    }
}
