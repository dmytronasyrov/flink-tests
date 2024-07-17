package app.shoppingcart;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.time.Instant;

public class EventGenerator<T> extends RichParallelSourceFunction<T> {

  private Integer sleepMillisBetweenEvents;
  private Instant baseInstant;
  private Generator<Long, T> generator;
  private boolean running = true;
  private Long extraDelayInMillisOnEveryTenEvents;

  public EventGenerator(final Integer sleepMillisBetweenEvents, final Instant baseInstant, final Generator<Long, T> generator, final Long extraDelayInMillisOnEveryTenEvents) {
    this.sleepMillisBetweenEvents = sleepMillisBetweenEvents;
    this.baseInstant = baseInstant;
    this.generator = generator;
    this.extraDelayInMillisOnEveryTenEvents = extraDelayInMillisOnEveryTenEvents;
  }

  public EventGenerator() {
  }

  private void run(Long id, SourceFunction.SourceContext<T> sourceContext) {
    if (running) {
      sourceContext.collect(generator.accept(id));
      sourceContext.emitWatermark(new Watermark(baseInstant.plusSeconds(id).toEpochMilli()));

      try {
        Thread.sleep(sleepMillisBetweenEvents);

        if (id % 10 == 0) {
          Thread.sleep(extraDelayInMillisOnEveryTenEvents);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      run(id + 1, sourceContext);
    }
  }

  @Override
  public void run(final SourceContext<T> sourceContext) throws Exception {
    run(1L, sourceContext);
  }

  @Override
  public void cancel() {
    running = false;
  }

  public Integer getSleepMillisBetweenEvents() {
    return sleepMillisBetweenEvents;
  }

  public void setSleepMillisBetweenEvents(final Integer sleepMillisBetweenEvents) {
    this.sleepMillisBetweenEvents = sleepMillisBetweenEvents;
  }

  public Instant getBaseInstant() {
    return baseInstant;
  }

  public void setBaseInstant(final Instant baseInstant) {
    this.baseInstant = baseInstant;
  }

  public Generator<Long, T> getGenerator() {
    return generator;
  }

  public void setGenerator(final Generator<Long, T> generator) {
    this.generator = generator;
  }

  public boolean isRunning() {
    return running;
  }

  public void setRunning(final boolean running) {
    this.running = running;
  }

  public Long getExtraDelayInMillisOnEveryTenEvents() {
    return extraDelayInMillisOnEveryTenEvents;
  }

  public void setExtraDelayInMillisOnEveryTenEvents(final Long extraDelayInMillisOnEveryTenEvents) {
    this.extraDelayInMillisOnEveryTenEvents = extraDelayInMillisOnEveryTenEvents;
  }
}
