package io.gtfs;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class EventTimeTest {
  @Test void initialWatermarkDoesNotRejectEveryEvent() {
    assertFalse(LakehouseJob.beyondRetention(1700000000000L, Long.MIN_VALUE, 120000));
    assertFalse(LakehouseJob.beyondRetention(1000, 120999, 120000));
    assertTrue(LakehouseJob.beyondRetention(1000, 121000, 120000));
  }
}
