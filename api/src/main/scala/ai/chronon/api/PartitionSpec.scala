/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.api

import ai.chronon.api.Extensions._

import java.text.SimpleDateFormat
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.TimeZone

case class PartitionSpec(column: String, format: String, spanMillis: Long) {

  private def partitionFormatter =
    DateTimeFormatter
      .ofPattern(format, Locale.US)
      .withZone(ZoneOffset.UTC)

  private def sdf = {
    val formatter = new SimpleDateFormat(format)
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    formatter
  }

  def epochMillis(partition: String): Long = {
    sdf.parse(partition).getTime
  }

  // what is the date portion of this timestamp
  def at(millis: Long): String = partitionFormatter.format(Instant.ofEpochMilli(millis))

  def before(s: String): String = shift(s, -1)

  def minus(s: String, window: Window): String = at(epochMillis(s) - window.millis)

  def plus(s: String, window: Window): String = at(epochMillis(s) + window.millis)

  def minus(partition: String, window: Option[Window]): String = {
    if (partition == null) return null
    window.map(minus(partition, _)).getOrElse(partition)
  }

  def plus(partition: String, window: Option[Window]): String = {
    if (partition == null) return null
    window.map(plus(partition, _)).getOrElse(partition)
  }

  def after(s: String): String = shift(s, 1)

  // all partitions `count` ahead of `s` including `s` - result size will be count + 1
  // used to compute effected output partitions for a given partition
  def partitionsFrom(s: String, count: Int): Seq[String] = s +: (1 to count).map(shift(s, _))

  def partitionsFrom(s: String, window: Window): Seq[String] = {
    val count = math.ceil(window.millis.toDouble / spanMillis).toInt
    partitionsFrom(s, count)
  }

  def before(millis: Long): String = at(millis - spanMillis)

  def shift(date: String, days: Int): String =
    partitionFormatter.format(Instant.ofEpochMilli(epochMillis(date) + days * spanMillis))

  def now: String = at(System.currentTimeMillis())

  def shiftBackFromNow(days: Int): String = shift(now, 0 - days)

  def intervalWindow: Window = {
    if (spanMillis == WindowUtils.Day.millis) WindowUtils.Day
    else if (spanMillis == WindowUtils.Hour.millis) WindowUtils.Hour
    else
      throw new UnsupportedOperationException(
        s"Partition Intervals should be either hour or day - found ${spanMillis / 60 * 1000} minutes")
  }

  def translate(date: String, targetSpec: PartitionSpec): String = {
    val millis = epochMillis(date)
    targetSpec.at(millis)
  }
}

object PartitionSpec {
  val daily: PartitionSpec = PartitionSpec("ds", "yyyy-MM-dd", 24 * 60 * 60 * 1000)
}
