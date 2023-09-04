package io.glutenproject.ldps.backend;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;

public abstract class GeneralInIterator implements AutoCloseable {
    protected final Iterator<ColumnarBatch> delegated;
    private transient ColumnarBatch nextBatch = null;

    public GeneralInIterator(Iterator<ColumnarBatch> delegated) {
        this.delegated = delegated;
    }

    public boolean hasNext() {
        while (delegated.hasNext()) {
            nextBatch = delegated.next();
            if (nextBatch.numRows() > 0) {
                // any problem using delegated.hasNext() instead?
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() throws Exception {
    }

    public ColumnarBatch nextColumnarBatch() {
        return nextBatch;
    }
}