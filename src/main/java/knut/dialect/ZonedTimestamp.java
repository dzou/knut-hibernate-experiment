/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package knut.dialect;

import java.sql.Timestamp;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import org.threeten.bp.Instant;
import org.threeten.bp.ZoneId;
import org.threeten.bp.ZonedDateTime;

/**
 * Cloud Spanner does not store timezone information together with a timestamp. This embeddable
 * class uses two columns to store both a timestamp and timezone information. Use this in an entity
 * when you want a timestamp with timezone.
 */
@Embeddable
public class ZonedTimestamp {
  @Column
  private Timestamp timestamp;

  @Column(length = 6)
  private String timezone;

  public ZonedTimestamp() {}

  public ZonedDateTime getZonedDateTime() {
    return ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(timestamp.getTime() * 1000L, timestamp.getNanos()),
        ZoneId.of(timezone));
  }

  public void setZonedDateTime(ZonedDateTime dateTime) {
    Instant instant = dateTime.toInstant();
    Timestamp ts = new Timestamp(instant.toEpochMilli());
    ts.setNanos(instant.getNano());
    this.timestamp = ts;
    this.timezone = dateTime.getZone().getId();
  }
}
