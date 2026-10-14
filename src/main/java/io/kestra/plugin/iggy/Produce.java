package io.kestra.plugin.iggy;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.topic.CompressionAlgorithm;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish messages to Apache Iggy stream and topic",
    description = "Publishes one or more messages to an Apache Iggy stream and topic using data provided via `from`. Supports lists, maps, or storage files."
)
@Plugin(
    examples = {
        @Example(
            title = "Produce messages to an Apache Iggy topic using username and password authentication",
            full = true,
            code = """
                id: iggy_produce
                namespace: company.team
                tasks:
                  - id: produce
                    type: io.kestra.plugin.iggy.Produce
                    host: iggy.example.com
                    port: 8090
                    username: "{{ secret('IGGY_USERNAME') }}"
                    password: "{{ secret('IGGY_PASSWORD') }}"
                    stream: orders
                    topic: created
                    from:
                      - payload: '{"orderId": 1, "total": 42.5}'
                      - payload: '{"orderId": 2, "total": 13.0}'
                """
        ),
        @Example(
            title = "Produce messages from an internal storage file to an Apache Iggy topic",
            full = true,
            code = """
                id: iggy_produce_from_file
                namespace: company.team
                tasks:
                  - id: produce
                    type: io.kestra.plugin.iggy.Produce
                    host: iggy.example.com
                    port: 8090
                    username: "{{ secret('IGGY_USERNAME') }}"
                    password: "{{ secret('IGGY_PASSWORD') }}"
                    stream: orders
                    topic: created
                    from: "{{ outputs.some_task_with_output_file.uri }}"
                """
        )
    }
)
public class Produce extends IggyConnection implements RunnableTask<Produce.Output>, Data.From {
    @NotNull
    @Schema(
        title = "Stream identifier or name",
        description = "Name or ID of the Apache Iggy stream to publish messages to."
    )
    @PluginProperty(group = "main")
    private Property<String> stream;

    @NotNull
    @Schema(
        title = "Topic identifier or name",
        description = "Name or ID of the Apache Iggy topic to publish messages to."
    )
    @PluginProperty(group = "main")
    private Property<String> topic;

    @Schema(
        title = "Partition ID",
        description = "Optional partition ID to publish to. If omitted, balanced partitioning is used."
    )
    @PluginProperty(group = "main")
    private Property<Long> partitionId;

    @NotNull
    @Schema(
        title = io.kestra.core.models.property.Data.From.TITLE,
        description = io.kestra.core.models.property.Data.From.DESCRIPTION,
        anyOf = { String.class, List.class, Map.class }
    )
    @PluginProperty(group = "main", internalStorageURI = true)
    private Object from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        IggyBaseClient client = createClient(runContext);
        try {
            String streamStr = runContext.render(this.stream).as(String.class).orElseThrow();
            String topicStr = runContext.render(this.topic).as(String.class).orElseThrow();

            StreamId streamId = resolveStreamId(streamStr);
            TopicId topicId = resolveTopicId(topicStr);

            ensureStreamAndTopic(runContext, client, streamId, streamStr, topicId, topicStr);

            Partitioning partitioning = this.partitionId != null ?
                Partitioning.partitionId(runContext.render(this.partitionId).as(Long.class).orElseThrow()) :
                Partitioning.balanced();

            int messagesCount = Data.from(this.from).read(runContext)
                .map(throwFunction(this::toMessage))
                .buffer(100)
                .map(throwFunction(batch -> {
                    client.messages().sendMessages(streamId, topicId, partitioning, batch);
                    return batch.size();
                }))
                .reduce(Integer::sum)
                .blockOptional()
                .orElse(0);

            return Output.builder()
                .messagesCount(messagesCount)
                .build();
        } finally {
            closeClient(client);
        }
    }

    private void ensureStreamAndTopic(RunContext runContext, IggyBaseClient client, StreamId streamId, String streamStr, TopicId topicId, String topicStr) {
        try {
            if (client.streams().getStream(streamId).isEmpty()) {
                try {
                    client.streams().createStream(streamStr);
                } catch (Exception e) {
                    runContext.logger().debug("Could not auto-create stream '{}': {}", streamStr, e.getMessage());
                }
            }
        } catch (Exception e) {
            runContext.logger().debug("Could not inspect stream '{}': {}", streamStr, e.getMessage());
        }

        try {
            if (client.topics().getTopic(streamId, topicId).isEmpty()) {
                try {
                    client.topics().createTopic(streamId, 1L, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO, Optional.empty(), topicStr);
                } catch (Exception e) {
                    runContext.logger().debug("Could not auto-create topic '{}': {}", topicStr, e.getMessage());
                }
            }
        } catch (Exception e) {
            runContext.logger().debug("Could not inspect topic '{}': {}", topicStr, e.getMessage());
        }
    }

    private Message toMessage(Object object) throws Exception {
        if (object instanceof Map<?, ?> map) {
            Object payloadObj = map.get("payload");
            if (payloadObj == null) {
                payloadObj = map.get("data");
            }
            if (payloadObj == null) {
                payloadObj = map.get("value");
            }

            String payloadStr;
            if (payloadObj == null) {
                payloadStr = JacksonMapper.ofJson().writeValueAsString(map);
            } else if (payloadObj instanceof String str) {
                payloadStr = str;
            } else if (payloadObj instanceof byte[] bytes) {
                payloadStr = new String(bytes, StandardCharsets.UTF_8);
            } else {
                payloadStr = JacksonMapper.ofJson().writeValueAsString(payloadObj);
            }

            Map<HeaderKey, HeaderValue> headers = new HashMap<>();
            Object headersObj = map.get("headers");
            if (headersObj instanceof Map<?, ?> headersMap) {
                for (Map.Entry<?, ?> entry : headersMap.entrySet()) {
                    if (entry.getKey() != null && entry.getValue() != null) {
                        headers.put(
                            HeaderKey.fromString(entry.getKey().toString()),
                            HeaderValue.fromString(entry.getValue().toString())
                        );
                    }
                }
            }

            return Message.of(payloadStr, headers.isEmpty() ? null : headers);
        } else if (object instanceof String str) {
            return Message.of(str);
        } else if (object instanceof byte[] bytes) {
            return Message.of(new String(bytes, StandardCharsets.UTF_8));
        } else {
            return Message.of(JacksonMapper.ofJson().writeValueAsString(object));
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Messages produced",
            description = "Total number of messages published during this task execution."
        )
        private final Integer messagesCount;
    }
}
