// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::error::{DataFusionError, Result};
use crate::physical_plan::functions::{TypeSignature, Volatility};
use crate::physical_plan::{
  aggregates, functions::Signature, type_coercion::data_types,
  windows::find_ranges_in_range, PhysicalExpr,
};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
pub use datafusion_expr::{BuiltInWindowFunction, WindowFunction};
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

/// Partition evaluator
pub(crate) trait PartitionEvaluator {
  /// Whether the evaluator should be evaluated with rank
  fn include_rank(&self) -> bool {
    false
  }

  /// evaluate the partition evaluator against the partitions
  fn evaluate(&self, partition_points: Vec<Range<usize>>) -> Result<Vec<ArrayRef>> {
    partition_points
      .into_iter()
      .map(|partition| self.evaluate_partition(partition))
      .collect()
  }

  /// evaluate the partition evaluator against the partitions with rank information
  fn evaluate_with_rank(
    &self,
    partition_points: Vec<Range<usize>>,
    sort_partition_points: Vec<Range<usize>>,
  ) -> Result<Vec<ArrayRef>> {
    partition_points
      .into_iter()
      .map(|partition| {
        let ranks_in_partition = find_ranges_in_range(&partition, &sort_partition_points);
        self.evaluate_partition_with_rank(partition, ranks_in_partition)
      })
      .collect()
  }

  /// evaluate the partition evaluator against the partition
  fn evaluate_partition(&self, _partition: Range<usize>) -> Result<ArrayRef>;

  /// evaluate the partition evaluator against the partition but with rank
  fn evaluate_partition_with_rank(
    &self,
    _partition: Range<usize>,
    _ranks_in_partition: &[Range<usize>],
  ) -> Result<ArrayRef> {
    Err(DataFusionError::NotImplemented(
      "evaluate_partition_with_rank is not implemented by default".into(),
    ))
  }
}
