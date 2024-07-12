package app.game;

import java.time.Instant;
import java.util.UUID;

public abstract class ServerEvent {

  Instant eventTime;
  String id;

  public ServerEvent(final Instant eventTime, final String id) {
    this.eventTime = eventTime;
    this.id = id;
  }

  public ServerEvent() {
  }

  public Instant getEventTime() {
    return eventTime;
  }

  public String getId() {
    return id;
  }

  public void setEventTime(final Instant eventTime) {
    this.eventTime = eventTime;
  }

  public void setId(final String id) {
    this.id = id;
  }
}
