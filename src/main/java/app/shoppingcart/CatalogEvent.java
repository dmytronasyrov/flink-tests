package app.shoppingcart;

import java.time.Instant;

public class CatalogEvent {
  private String userId;
  private Instant time;

  public CatalogEvent() {
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(final String userId) {
    this.userId = userId;
  }

  public Instant getTime() {
    return time;
  }

  public void setTime(final Instant time) {
    this.time = time;
  }
}
