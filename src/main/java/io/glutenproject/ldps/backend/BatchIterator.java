package io.glutenproject.ldps.backend;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import io.glutenproject.ldps.utils.CHExecUtil;

import java.io.IOException;
import java.util.List;

public class BatchIterator extends GeneralOutIterator {
    private final long handle;

    public BatchIterator(long handle, List<Attribute> outAttrs) throws IOException {
        super(outAttrs);
        this.handle = handle;
    }

    private native boolean nativeHasNext(long nativeHandle);

    private native byte[] nativeNext(long nativeHandle);

    private native long nativeCHNext(long nativeHandle);

    private native void nativeClose(long nativeHandle);

    private native IMetrics nativeFetchMetrics(long nativeHandle);

    @Override
    public boolean hasNextInternal() throws IOException {
        return nativeHasNext(handle);
    }

    @Override
    public ColumnarBatch nextInternal() throws IOException {
        long block = nativeCHNext(handle);
        CHNativeBlock nativeBlock = new CHNativeBlock(block);
        int cols = nativeBlock.numColumns();
        ColumnVector[] columnVectors = new ColumnVector[cols];
        for (int i = 0; i < cols; i++) {
            columnVectors[i] = new CHColumnVector(CHExecUtil.inferSparkDataType(
                    nativeBlock.getTypeByPosition(i)), block, i);
        }
        return new ColumnarBatch(columnVectors, nativeBlock.numRows());
    }

    @Override
    public IMetrics getMetricsInternal() throws IOException, ClassNotFoundException {
        return nativeFetchMetrics(handle);
    }

    @Override
    public void closeInternal() {
        nativeClose(handle);
    }
}