package app;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.net.Socket;
import java.util.Random;
import java.util.Scanner;

public class CustomSources {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  //////////////////////////////////////////////////

  public static void main(String[] args) throws Exception {
//    final DataStreamSource<Long> longDataStreamSource = env.addSource(new RandomNumberGeneratorSource(2))
//      .setParallelism(10);
//    longDataStreamSource.print();

    final SocketStringSource source = new SocketStringSource("localhost", 31566);
    final DataStreamSource<String> stream = env.addSource(source);
    stream.print();

    env.execute();
  }

  //////////////////////////////////////////////////

  public static final class RandomNumberGeneratorSource extends RichParallelSourceFunction<Long> {

    private final double maxSleepTime;
    private final Random random = new Random();
    private boolean isRunning = true;

    public RandomNumberGeneratorSource(double minEventsPerSecond) {
      maxSleepTime = (1000.0 / minEventsPerSecond);
    }

    @Override
    public void run(final SourceContext<Long> sourceContext) throws Exception {
      while (isRunning) {
        final long sleepTime = (long) Math.abs(random.nextLong() % maxSleepTime);
        final long nextNumber = random.nextLong();
        Thread.sleep(sleepTime);
        sourceContext.collect(nextNumber);
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }

    @Override
    public void open(final OpenContext openContext) throws Exception {
      final String output = String.format("[%s] starting source function", Thread.currentThread().getName());
      System.out.println(output);
    }

    @Override
    public void close() throws Exception {
      final String output = String.format("[%s] closing source function", Thread.currentThread().getName());
      System.out.println(output);
    }
  }

  //////////////////////////////////////////////////

  public static final class SocketStringSource extends RichSourceFunction<String> {

    private final String host;
    private final int port;
    private Socket socket;
    private boolean isRunning = true;

    public SocketStringSource(final String host, final int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public void open(final OpenContext openContext) throws Exception {
      socket = new Socket(host, port);
    }

    @Override
    public void close() throws Exception {
      socket.close();
    }

    @Override
    public void run(final SourceContext<String> sourceContext) throws Exception {
      final Scanner scanner = new Scanner(socket.getInputStream());

      while (isRunning && scanner.hasNextLine()) {
        sourceContext.collect(scanner.nextLine());
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }
}
