package app;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectSinkFunction;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

public class KafkaIntegration {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final KafkaSource<NftRank> kafkaSource = KafkaSource.<NftRank>builder()
      .setBootstrapServers("kafka-data-broker-0.ludo.ninja:9400,kafka-data-broker-1.ludo.ninja:9401,kafka-data-broker-2.ludo.ninja:9402,kafka-data-broker-3.ludo.ninja:9403,kafka-data-broker-4.ludo.ninja:9404")
      .setTopics("rank-nft")
      .setGroupId("flink-dev")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer((DeserializationSchema) new NftRankDeserializer())
      .build();

    final DataStreamSource<NftRank> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
    source.print();

    final KafkaRecordSerializationSchema<NftRank> serializationSchema = KafkaRecordSerializationSchema.<NftRank>builder()
      .setTopic("test-rank-nft")
      .setValueSerializationSchema(new SerializationSchema<NftRank>() {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public byte[] serialize(final NftRank nftRank) {
          try {
            return objectMapper.writeValueAsBytes(nftRank);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        }
      })
      .build();
    final KafkaSink<NftRank> sink = KafkaSink.<NftRank>builder()
      .setBootstrapServers("http://kafka-data-broker-0.ludo.ninja:9400,http://kafka-data-broker-1.ludo.ninja:9401,http://kafka-data-broker-2.ludo.ninja:9402,http://kafka-data-broker-3.ludo.ninja:9403,http://kafka-data-broker-4.ludo.ninja:9404")
      .setRecordSerializer(serializationSchema)
      .build();
    source.sinkTo(sink);

    env.execute();
  }

  public static final class NftRankDeserializer implements DeserializationSchema<NftRank> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public NftRankDeserializer() {
    }

    @Override
    public void open(final InitializationContext context) throws Exception {
      DeserializationSchema.super.open(context);

      SimpleModule module = new SimpleModule();
      module.addDeserializer(Long.class, new CustomRankUpdatedAtDeserializer());
      objectMapper.registerModules(module);
    }

    @Override
    public NftRank deserialize(final byte[] bytes) throws IOException {
      return objectMapper.reader().readValue(bytes, NftRank.class);
    }

    @Override
    public boolean isEndOfStream(final NftRank nftRank) {
      return false;
    }

    @Override
    public TypeInformation<NftRank> getProducedType() {
      return TypeInformation.of(NftRank.class);
    }
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public static final class NftRank {
    private Float rank;
    private Float rankRaw;
    private Float rankMax;
    @JsonDeserialize(using = CustomRankUpdatedAtDeserializer.class)
    private Long rankUpdatedAt;
    private String version;

    public NftRank(final Float rank, final Float rankRaw, final Float rankMax, final Long rankUpdatedAt, final String version) {
      this.rank = rank;
      this.rankRaw = rankRaw;
      this.rankMax = rankMax;
      this.rankUpdatedAt = rankUpdatedAt;
      this.version = version;
    }

    public NftRank() {
    }

    public Float getRank() {
      return rank;
    }

    public void setRank(final Float rank) {
      this.rank = rank;
    }

    public Float getRankRaw() {
      return rankRaw;
    }

    public void setRankRaw(final Float rankRaw) {
      this.rankRaw = rankRaw;
    }

    public Float getRankMax() {
      return rankMax;
    }

    public void setRankMax(final Float rankMax) {
      this.rankMax = rankMax;
    }

    public Long getRankUpdatedAt() {
      return rankUpdatedAt;
    }

    public void setRankUpdatedAt(final Long rankUpdatedAt) {
      this.rankUpdatedAt = rankUpdatedAt;
    }

    public String getVersion() {
      return version;
    }

    public void setVersion(final String version) {
      this.version = version;
    }

    @Override
    public String toString() {
      return "NftRank{" +
        "rank=" + rank +
        ", rankRaw=" + rankRaw +
        ", rankMax=" + rankMax +
        ", rankUpdatedAt=" + rankUpdatedAt +
        ", version='" + version + '\'' +
        '}';
    }
  }

  public static class CustomRankUpdatedAtDeserializer extends JsonDeserializer<Long> {

    public CustomRankUpdatedAtDeserializer() {
    }

    @Override
    public Long deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      final String rankUpdatedAtStr = p.getText();

      if (rankUpdatedAtStr == null)
        return null;

      try {
        return Long.valueOf(rankUpdatedAtStr);
      } catch (NumberFormatException e1) {

        try {
          final SimpleDateFormat newDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
          final Date rankUpdatedAtDate = newDateFormat.parse(rankUpdatedAtStr);
          final Instant rankUpdatedAt = rankUpdatedAtDate.toInstant();
          return rankUpdatedAt.toEpochMilli();
        } catch (ParseException e2) {
          // there were some changes to the dateformat, let's try the old format

          try {
            final SimpleDateFormat initialDateFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
            final Date rankUpdatedAtDate = initialDateFormat.parse(rankUpdatedAtStr);
            final Instant rankUpdatedAt = rankUpdatedAtDate.toInstant();
            return rankUpdatedAt.toEpochMilli();
          } catch (ParseException ignored) {
            return null;
          }
        }
      }
    }
  }
}
