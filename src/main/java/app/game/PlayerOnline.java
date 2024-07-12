package app.game;

import java.time.Instant;
import java.util.UUID;

public class PlayerOnline extends ServerEvent {

  private String username;

  public PlayerOnline(final Instant eventTime, final String id, final String username) {
    super(eventTime, id);
    this.username = username;
  }

  public PlayerOnline() {
    super();
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(final String username) {
    this.username = username;
  }

  @Override
  public String toString() {
    return "PlayerOnline{" +
      "eventTime=" + eventTime +
      ", id=" + id +
      ", username='" + username + '\'' +
      '}';
  }
}
