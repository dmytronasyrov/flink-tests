package app;

import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

public class DataSender {

  public static void main(String[] args) throws IOException, InterruptedException {
    try (final ServerSocket serverSocket = new ServerSocket(31566)) {
      System.out.println("Waiting for Flink to connect...");

      try (final Socket socket = serverSocket.accept();
           final PrintStream printer = new PrintStream(socket.getOutputStream())) {
        System.out.println("Flink connected. Sending data...");

        printer.println("Hello from the other side...");
        Thread.sleep(3000);
        printer.println("Almost ready...");
        Thread.sleep(500);

        for (int i = 0; i < 10; i++) {
          printer.println("Number " + i);
        }

        System.out.println("Data sending completed.");
      }
    }
  }
}
