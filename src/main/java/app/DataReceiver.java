package app;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class DataReceiver {

  public static void main(String[] args) throws IOException {
    final ServerSocket server = new ServerSocket(12345);
    final Socket socket = server.accept();
    final Scanner reader = new Scanner(socket.getInputStream());
    System.out.println("Flink connected. Reading...");

    while (reader.hasNextLine()) {
      System.out.println("> " + reader.nextLine());
    }

    socket.close();
    System.out.println("All data read. Closing");
    server.close();
  }
}
