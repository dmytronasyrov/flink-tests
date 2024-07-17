package app.shoppingcart;

import java.time.Instant;

public class ProductDetailsViewed extends CatalogEvent {
  private String userId;
  private Instant time;
  private String productId;

  public ProductDetailsViewed(final String userId, final Instant time, final String productId) {
    this.userId = userId;
    this.time = time;
    this.productId = productId;
  }

  public ProductDetailsViewed() {
    super();
  }

  public String getUserId() {
    return userId;
  }

  public Instant getTime() {
    return time;
  }

  public String getProductId() {
    return productId;
  }

  @Override
  public void setUserId(final String userId) {
    this.userId = userId;
  }

  @Override
  public void setTime(final Instant time) {
    this.time = time;
  }

  public void setProductId(final String productId) {
    this.productId = productId;
  }
}
