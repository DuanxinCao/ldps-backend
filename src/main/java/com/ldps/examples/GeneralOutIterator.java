package com.ldps.examples;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
public abstract class GeneralOutIterator implements AutoCloseable, Serializable {
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final transient List<Attribute> outAttrs;

    public GeneralOutIterator(List<Attribute> outAttrs) {
        this.outAttrs = outAttrs;
    }

    public final boolean hasNext() throws Exception {
        return hasNextInternal();
    }

    public final ColumnarBatch next() throws Exception {
        return nextInternal();
    }

    public final IMetrics getMetrics() throws Exception {
        return getMetricsInternal();
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            closeInternal();
        }
    }

    protected abstract void closeInternal();

    protected abstract boolean hasNextInternal() throws Exception;

    protected abstract ColumnarBatch nextInternal() throws Exception;

    protected abstract IMetrics getMetricsInternal() throws Exception;

}