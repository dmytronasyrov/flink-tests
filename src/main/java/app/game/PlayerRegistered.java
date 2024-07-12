package app.game;

import java.time.Instant;
import java.util.UUID;

public class PlayerRegistered extends ServerEvent {

  private String username;

  public PlayerRegistered(Instant eventTime, String id, String username) {
    super(eventTime, id);
    this.username = username;
  }

  public PlayerRegistered() {
    super();
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  @Override
  public String toString() {
    return "PlayerRegistered{" +
      "eventTime=" + eventTime +
      ", id=" + id +
      ", username='" + username + '\'' +
      '}';
  }
}
