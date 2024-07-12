package app;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class StreamingJob {

  private SourceFunction<Long> source;
  private SinkFunction<Long> sink;

  public StreamingJob(SourceFunction<Long> source, SinkFunction<Long> sink) {
    this.source = source;
    this.sink = sink;
  }

  public void execute() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.addSource(source)
      .returns(TypeInformation.of(Long.class))
      .map(new IncrementMapFunction())
      .addSink(sink);

    env.execute();
  }

  public static void main(String[] args) throws Exception {
    final StreamingJob job = new StreamingJob(new RandomLongSource(), new PrintSinkFunction<>());
    job.execute();
  }

  public class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
      return record + 1;
    }
  }
}
