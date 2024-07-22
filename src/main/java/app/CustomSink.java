package app;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class CustomSink {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private static final DataStreamSource<String> stream = env.fromElements(
    "privet 1",
    "privet 2",
    "privet 3",
    "privet 4",
    "privet 5"
  );

  //////////////////////////////////////////////////

  public static void main(String[] args) throws Exception {
//    stream.addSink(new FileSink("output/demoFileSink"));
//    stream.print();

    stream.addSink(new SocketSink("localhost", 12345)).setParallelism(1);
    stream.print();

    env.execute();
  }

  public static final class FileSink extends RichSinkFunction<String> {

    private final String path;
    private PrintWriter writer;

    public FileSink(final String path) {
      this.path = path;
    }

    @Override
    public void open(final OpenContext openContext) throws Exception {
      writer = new PrintWriter(new FileWriter(path, true));
    }

    @Override
    public void invoke(final String value, final Context context) throws Exception {
      writer.println(value);
      writer.flush();
    }

    @Override
    public void close() throws Exception {
      writer.close();
    }
  }

  private static final class SocketSink extends RichSinkFunction<String> {
    private final String host;
    private final int port;
    private Socket socket;
    private PrintWriter writer;

    public SocketSink(final String host, final int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public void close() throws Exception {
      socket.close();
    }

    @Override
    public void open(final OpenContext openContext) throws Exception {
      socket = new Socket(host, port);
      writer = new PrintWriter(socket.getOutputStream());
    }

    @Override
    public void invoke(final String value, final Context context) throws Exception {
      writer.println(value);
      writer.flush();
    }
  }
}
