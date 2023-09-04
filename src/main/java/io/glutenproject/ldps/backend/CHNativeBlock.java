package io.glutenproject.ldps.backend;

import io.glutenproject.ldps.utils.CHExecUtil;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class CHNativeBlock {
    private long blockAddress;

    public CHNativeBlock(long blockAddress) {
        this.blockAddress = blockAddress;
    }

    public static CHNativeBlock fromColumnarBatch(ColumnarBatch batch) {
        if (batch.numCols() == 0 || !(batch.column(0) instanceof CHColumnVector)) {
            throw new RuntimeException(
                    "Unexpected ColumnarBatch: " + (batch.numCols() == 0 ?
                            "0 column" : "expected CHColumnVector, but " + batch.column(0).getClass()));
        }
        CHColumnVector columnVector = (CHColumnVector) batch.column(0);
        return new CHNativeBlock(columnVector.getBlockAddress());
    }

    private native int nativeNumRows(long blockAddress);

    public int numRows() {
        return nativeNumRows(blockAddress);
    }

    public long blockAddress() {
        return blockAddress;
    }

    private native int nativeNumColumns(long blockAddress);

    public int numColumns() {
        return nativeNumColumns(blockAddress);
    }

    private native String nativeColumnType(long blockAddress, int position);

    public String getTypeByPosition(int position) {
        return nativeColumnType(blockAddress, position);
    }

    private native long nativeTotalBytes(long blockAddress);

    public long totalBytes() {
        return nativeTotalBytes(blockAddress);
    }

    public native void nativeClose(long blockAddress);

    public void close() {
        if (blockAddress != 0) {
            nativeClose(blockAddress);
            blockAddress = 0;
        }
    }

    public ColumnarBatch toColumnarBatch() {
        ColumnVector[] vectors = new ColumnVector[numColumns()];
        for (int i = 0; i < numColumns(); i++) {
            vectors[i] = new CHColumnVector(CHExecUtil.inferSparkDataType(
                    getTypeByPosition(i)), blockAddress, i);
        }
        int numRows = 0;
        if (numColumns() != 0) {
            numRows = numRows();
        }
        return new ColumnarBatch(vectors, numRows);
    }
}