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

case class CumulatedBatchIr(columns: Array[CumulatedBatchColumn],
                            collapsed: Array[Any],
                            suffixColumnIndices: Array[Int])

sealed trait CumulatedBatchColumn
case class StaticBatchColumn(value: Any) extends CumulatedBatchColumn
case class SuffixBatchColumn(timestamps: Array[Long], values: Array[Any], collapsed: Any) extends CumulatedBatchColumn

case class CumulatedBatchOptions(skipSmallWindowBatchTails: Boolean)

object CumulatedBatchOptions {
  // Standard sawtooth serving only reads streaming data after batchEnd, so small windows
  // still need their batch-side tail suffixes when they cross batchEnd.
  val SawtoothServing: CumulatedBatchOptions = CumulatedBatchOptions(skipSmallWindowBatchTails = false)

  // MegaTile serving keeps small windows fully in streaming megatiles; batch tails
  // must be skipped there or they will be counted twice.
  val MegaTileServing: CumulatedBatchOptions = CumulatedBatchOptions(skipSmallWindowBatchTails = true)
}
