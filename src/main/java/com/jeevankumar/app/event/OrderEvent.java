package com.jeevankumar.app.event;

import java.time.Instant;

public record OrderEvent(String orderId, int seq, String eventType, Instant ts) {
}
