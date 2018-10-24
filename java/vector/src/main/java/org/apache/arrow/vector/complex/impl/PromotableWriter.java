/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.complex.impl;

import java.math.BigDecimal;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.AbstractStructVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/**
 * This FieldWriter implementation delegates all FieldWriter API calls to an inner FieldWriter. This inner field writer
 * can start as a specific type, and this class will promote the writer to a UnionWriter if a call is made that the
 * specifically typed writer cannot handle. A new UnionVector is created, wrapping the original vector, and replaces the
 * original vector in the parent vector, which can be either an AbstractStructVector or a ListVector.
 */
public class PromotableWriter extends AbstractPromotableFieldWriter {

  private final AbstractStructVector parentContainer;
  private final ListVector listVector;
  private final NullableStructWriterFactory nullableStructWriterFactory;
  private int position;
  private final static int MAX_DECIMAL_PRECISION = 38;

  private enum State {
    UNTYPED, SINGLE, UNION
  }

  private MinorType type;
  private ArrowType arrowType;
  private ValueVector vector;
  private UnionVector unionVector;
  private State state;
  private FieldWriter writer;

  public PromotableWriter(ValueVector v, AbstractStructVector parentContainer) {
    this(v, parentContainer, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  public PromotableWriter(
      ValueVector v,
      AbstractStructVector parentContainer,
      NullableStructWriterFactory nullableStructWriterFactory) {
    this.parentContainer = parentContainer;
    this.listVector = null;
    this.nullableStructWriterFactory = nullableStructWriterFactory;
    init(v);
  }

  public PromotableWriter(ValueVector v, ListVector listVector) {
    this(v, listVector, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  public PromotableWriter(
      ValueVector v,
      ListVector listVector,
      NullableStructWriterFactory nullableStructWriterFactory) {
    this.listVector = listVector;
    this.parentContainer = null;
    this.nullableStructWriterFactory = nullableStructWriterFactory;
    init(v);
  }

  private void init(ValueVector v) {
    if (v instanceof UnionVector) {
      state = State.UNION;
      unionVector = (UnionVector) v;
      writer = new UnionWriter(unionVector, nullableStructWriterFactory);
    } else if (v instanceof ZeroVector) {
      state = State.UNTYPED;
    } else {
      setWriter(v);
    }
  }

  private void setWriter(ValueVector v) {
    setWriter(v, null);
  }

  private void setWriter(ValueVector v, ArrowType arrowType) {
    state = State.SINGLE;
    vector = v;
    type = v.getMinorType();
    this.arrowType = arrowType;
    switch (type) {
      case STRUCT:
        writer = nullableStructWriterFactory.build((StructVector) vector);
        break;
      case LIST:
        writer = new UnionListWriter((ListVector) vector, nullableStructWriterFactory);
        break;
      case UNION:
        writer = new UnionWriter((UnionVector) vector, nullableStructWriterFactory);
        break;
      default:
        writer = type.getNewFieldWriter(vector);
        break;
    }
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    FieldWriter w = getWriter();
    if (w == null) {
      position = index;
    } else {
      w.setPosition(index);
    }
  }

  protected FieldWriter getWriter(MinorType type) {
    return getWriter(type, null);
  }

  protected FieldWriter getWriter(MinorType type, ArrowType arrowType) {
    if (state == State.UNION) {
      ((UnionWriter) writer).getWriter(type);
    } else if (state == State.UNTYPED) {
      if (type == null) {
        // ???
        return null;
      }
      if (arrowType == null) {
        arrowType = type.getType();
      }
      ValueVector v = listVector.addOrGetVector(FieldType.nullable(arrowType)).getVector();
      v.allocateNew();
      setWriter(v, arrowType);
      writer.setPosition(position);
    } else if (type != this.type) {
      promoteToUnion();
      ((UnionWriter) writer).getWriter(type);
    }
    return writer;
  }

  @Override
  public boolean isEmptyStruct() {
    return writer.isEmptyStruct();
  }

  protected FieldWriter getWriter() {
    return writer;
  }

  private FieldWriter promoteToUnion() {
    String name = vector.getField().getName();
    TransferPair tp = vector.getTransferPair(vector.getMinorType().name().toLowerCase(), vector.getAllocator());
    tp.transfer();
    if (parentContainer != null) {
      // TODO allow dictionaries in complex types
      unionVector = parentContainer.addOrGetUnion(name);
      unionVector.allocateNew();
    } else if (listVector != null) {
      unionVector = listVector.promoteToUnion();
    }
    unionVector.addVector((FieldVector) tp.getTo());
    writer = new UnionWriter(unionVector, nullableStructWriterFactory);
    writer.setPosition(idx());
    for (int i = 0; i <= idx(); i++) {
      unionVector.setType(i, vector.getMinorType());
    }
    vector = null;
    state = State.UNION;
    return writer;
  }

  @Override
  public void write(DecimalHolder holder) {
    // Infer decimal scale and precision
    if (arrowType == null) {
      arrowType = new ArrowType.Decimal(MAX_DECIMAL_PRECISION, holder.scale);
    }

    getWriter(MinorType.DECIMAL, arrowType).write(holder);
  }

  @Override
  public void writeDecimal(int start, ArrowBuf buffer) {
    // Cannot infer decimal scale and precision
    if (arrowType == null) {
      throw new IllegalStateException("Cannot infer decimal scale and precision");
    }

    getWriter(MinorType.DECIMAL, arrowType).writeDecimal(start, buffer);
  }

  @Override
  public void writeDecimal(BigDecimal value) {
    // Infer decimal scale and precision
    if (arrowType == null) {
      arrowType = new ArrowType.Decimal(MAX_DECIMAL_PRECISION, value.scale());
    }

    getWriter(MinorType.DECIMAL, arrowType).writeDecimal(value);
  }

  @Override
  public void allocate() {
    getWriter().allocate();
  }

  @Override
  public void clear() {
    getWriter().clear();
  }

  @Override
  public Field getField() {
    return getWriter().getField();
  }

  @Override
  public int getValueCapacity() {
    return getWriter().getValueCapacity();
  }

  @Override
  public void close() throws Exception {
    getWriter().close();
  }
}
