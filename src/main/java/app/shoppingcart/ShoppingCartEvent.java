package app.shoppingcart;

import java.time.Instant;

public abstract class ShoppingCartEvent {
  String userId;
  Instant time;
  String sku;
  Integer quantity;

  public ShoppingCartEvent(final String userId, final Instant time, final String sku, final Integer quantity) {
    this.userId = userId;
    this.time = time;
    this.sku = sku;
    this.quantity = quantity;
  }

  public ShoppingCartEvent() {
  }

  public String getUserId() {
    return userId;
  }

  public Instant getTime() {
    return time;
  }

  public String getSku() {
    return sku;
  }

  public Integer getQuantity() {
    return quantity;
  }

  public void setUserId(final String userId) {
    this.userId = userId;
  }

  public void setTime(final Instant time) {
    this.time = time;
  }

  public void setSku(final String sku) {
    this.sku = sku;
  }

  public void setQuantity(final Integer quantity) {
    this.quantity = quantity;
  }

  @Override
  public String toString() {
    return "ShoppingCartEvent{" +
      "userId='" + userId + '\'' +
      ", time=" + time +
      ", sku='" + sku + '\'' +
      ", quantity=" + quantity +
      '}';
  }
}
