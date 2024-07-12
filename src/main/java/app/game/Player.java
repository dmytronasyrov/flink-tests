package app.game;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class Player {
  private final UUID id;
  private final String username;

  public Player(String username) {
    this.id = UUID.randomUUID();
    this.username = username;
  }

  public ServerEvent register(Instant startTime, Duration duration) {
    final Instant time = startTime.plusMillis(duration.toMillis());
    return new PlayerRegistered(time, id.toString(), username);
  }

  public ServerEvent online(Instant startTime, Duration duration) {
    final Instant time = startTime.plusMillis(duration.toMillis());
    return new PlayerOnline(time, id.toString(), username);
  }
}
