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

//! Defines physical expression for `lead` and `lag` that can evaluated
//! at runtime during query execution

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    window_functions::BuiltInWindowFunctionExpr, PhysicalExpr, WindowAccumulator,
    WindowShift,
};
use crate::scalar::ScalarValue;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::sync::Arc;

/// lead expression
#[derive(Debug)]
pub struct Lead {
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl Lead {
    /// create a lead window expr
    pub fn new(name: String, data_type: DataType, expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            name,
            data_type,
            expr,
        }
    }
}

impl BuiltInWindowFunctionExpr for Lead {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = true;
        Ok(Field::new(&self.name, self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_accumulator(&self) -> Result<Box<dyn WindowAccumulator>> {
        Ok(Box::new(CopyAccumulator {
            // TODO add support for customized offset
            window_shift: WindowShift::Lead(1),
        }))
    }
}

/// lag expression
#[derive(Debug)]
pub struct Lag {
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl Lag {
    /// create a lag window expr
    pub fn new(name: String, data_type: DataType, expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            name,
            data_type,
            expr,
        }
    }
}

impl BuiltInWindowFunctionExpr for Lag {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = true;
        Ok(Field::new(&self.name, self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_accumulator(&self) -> Result<Box<dyn WindowAccumulator>> {
        Ok(Box::new(CopyAccumulator {
            // TODO add support for customized offset
            window_shift: WindowShift::Lag(1),
        }))
    }
}

/// copy accumulator is a simple accumulator that simply copies data from the input to the output
/// and combined with window shift we can implement lead/lag without peeking async sequence of
/// batches of data
#[derive(Debug)]
struct CopyAccumulator {
    window_shift: WindowShift,
}

impl WindowAccumulator for CopyAccumulator {
    fn scan(&mut self, values: &[ScalarValue]) -> Result<Option<ScalarValue>> {
        let value = values[0].clone();
        Ok(Some(value))
    }

    fn scan_batch(
        &mut self,
        _num_rows: usize,
        values: &[ArrayRef],
    ) -> Result<Option<ArrayRef>> {
        match values.len() {
            1 => Ok(Some(values[0].clone())),
            len => Err(DataFusionError::Internal(format!(
                "Invalid number of inputs, expected 1, got {}",
                len
            ))),
        }
    }

    fn evaluate(&self) -> Result<Option<ScalarValue>> {
        Ok(None)
    }

    fn window_shift(&self) -> Option<WindowShift> {
        Some(self.window_shift)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::physical_plan::expressions::col;

    #[test]
    fn lead_lag_window_shift() -> Result<()> {
        let lead = Arc::new(Lead::new("lead".to_owned(), DataType::Float32, col("c3")));
        let acc = lead.create_accumulator()?;
        assert_eq!(WindowShift::Lead(1), acc.window_shift().unwrap());

        let lag = Arc::new(Lag::new("lead".to_owned(), DataType::Float32, col("c3")));
        let acc = lag.create_accumulator()?;
        assert_eq!(WindowShift::Lag(1), acc.window_shift().unwrap());
        Ok(())
    }
}
