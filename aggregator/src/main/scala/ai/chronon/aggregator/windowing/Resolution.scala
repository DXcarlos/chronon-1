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

package ai.chronon.aggregator.windowing

import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.Aggregation
import ai.chronon.api.GroupBy
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window

trait Resolution extends Serializable {
  // For a given window what is the resolution of the tail
  // The tail hops with the window size as represented by the return value
  def calculateTailHop(window: Window): Long

  // What are the hops that we will use to tile the full window
  // 1. Need to be sorted in descending order, and
  // 2. Every element needs to be a multiple of the next one
  // 3. calculateTailHop needs to return values only from this.
  val hopSizes: Array[Long]
}

object FiveMinuteResolution extends Resolution {
  def calculateTailHop(window: Window): Long =
    window.millis match {
      case x if x >= new Window(12, TimeUnit.DAYS).millis  => WindowUtils.Day.millis
      case x if x >= new Window(12, TimeUnit.HOURS).millis => WindowUtils.Hour.millis
      case _                                               => WindowUtils.FiveMinutes
    }

  val hopSizes: Array[Long] =
    Array(WindowUtils.Day.millis, WindowUtils.Hour.millis, WindowUtils.FiveMinutes)
}

private[windowing] object MinuteWindowResolution extends Resolution {
  private val OneHourMillis = new Window(1, TimeUnit.HOURS).millis

  def calculateTailHop(window: Window): Long =
    window.millis match {
      case x if x <= 0                                     => WindowUtils.FiveMinutes
      case x if x >= new Window(12, TimeUnit.DAYS).millis  => WindowUtils.Day.millis
      case x if x >= new Window(12, TimeUnit.HOURS).millis => WindowUtils.Hour.millis
      case x if x >= OneHourMillis                         => WindowUtils.FiveMinutes
      case _                                               => WindowUtils.Minute
    }

  val hopSizes: Array[Long] =
    Array(WindowUtils.Day.millis, WindowUtils.Hour.millis, WindowUtils.FiveMinutes, WindowUtils.Minute)
}

object DailyResolution extends Resolution {

  def calculateTailHop(window: Window): Long =
    window.timeUnit match {
      case TimeUnit.DAYS => WindowUtils.Day.millis
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid request for window $window for daily aggregation. " +
            "Window can only be multiples of 1d or the operation needs to be un-windowed."
        )
    }

  val hopSizes: Array[Long] = Array(WindowUtils.Day.millis)
}

object ResolutionUtils {
  private val MinuteResolutionWindowThresholdMillis: Long = new Window(1, TimeUnit.HOURS).millis

  private def hasMinuteWindow(aggregations: Seq[Aggregation]): Boolean =
    Option(aggregations)
      .exists(_.exists(agg =>
        Option(agg.windows).exists(_.iterator().toScala.exists(window =>
          window != null && window.millis > 0 && window.millis < MinuteResolutionWindowThresholdMillis))))

  def effectiveResolution(aggregations: Seq[Aggregation], resolution: Resolution): Resolution =
    if ((resolution eq FiveMinuteResolution) && hasMinuteWindow(aggregations)) MinuteWindowResolution
    else resolution

  /** Find the smallest tail window resolution in a GroupBy. Returns 1D if the GroupBy does not define any windows (all-time aggregates).
    * The window resolutions are: 1 min for a GroupBy window < 1 hr, 5 min for < 12 hrs,
    * 1 hr for < 12 days, 1 day for > 12 days.
    */
  def getSmallestTailHopMillis(groupBy: GroupBy): Long = {
    val aggregations = Option(groupBy.aggregations).map(_.toScala.toSeq).getOrElse(Seq.empty)
    val resolution = effectiveResolution(aggregations, FiveMinuteResolution)

    val tailHops =
      for (
        aggs <- Option(groupBy.aggregations).toSeq;
        agg <- aggs.iterator().toScala;
        windows <- Option(agg.windows).toSeq;
        window <- windows.iterator().toScala
      ) yield {
        resolution.calculateTailHop(window)
      }

    if (tailHops.isEmpty) WindowUtils.Day.millis
    else tailHops.min

  }
}
