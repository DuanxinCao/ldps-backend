package io.glutenproject.ldps.backend;

import io.glutenproject.ldps.utils.CHExecUtil;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;

public class CHColumnVector extends ColumnVector implements Serializable{

    private int columnPosition;
    private long blockAddress;
    public CHColumnVector(){
        super(CHExecUtil.inferSparkDataType(null));
    }

    public CHColumnVector(DataType type, long blockAddress, int columnPosition) {
        super(type);
        this.blockAddress = blockAddress;
        this.columnPosition = columnPosition;
    }

    public long getBlockAddress() {
        return blockAddress;
    }

    @Override
    public void close() {
        // blockAddress = 0;
    }

    private native boolean nativeHasNull(long blockAddress, int columnPosition);

    @Override
    public boolean hasNull() {
        return nativeHasNull(blockAddress, columnPosition);
    }

    private native int nativeNumNulls(long blockAddress, int columnPosition);

    @Override
    public int numNulls() {
        return nativeNumNulls(blockAddress, columnPosition);
    }

    private native boolean nativeIsNullAt(int rowId, long blockAddress, int columnPosition);

    @Override
    public boolean isNullAt(int rowId) {
        return nativeIsNullAt(rowId, blockAddress, columnPosition);
    }

    private native boolean nativeGetBoolean(int rowId, long blockAddress, int columnPosition);

    @Override
    public boolean getBoolean(int rowId) {
        return nativeGetBoolean(rowId, blockAddress, columnPosition);
    }

    private native byte nativeGetByte(int rowId, long blockAddress, int columnPosition);

    @Override
    public byte getByte(int rowId) {
        return nativeGetByte(rowId, blockAddress, columnPosition);
    }

    private native short nativeGetShort(int rowId, long blockAddress, int columnPosition);

    @Override
    public short getShort(int rowId) {
        return nativeGetShort(rowId, blockAddress, columnPosition);
    }

    private native int nativeGetInt(int rowId, long blockAddress, int columnPosition);

    @Override
    public int getInt(int rowId) {
        return nativeGetInt(rowId, blockAddress, columnPosition);
    }

    private native long nativeGetLong(int rowId, long blockAddress, int columnPosition);

    @Override
    public long getLong(int rowId) {
        return nativeGetLong(rowId, blockAddress, columnPosition);
    }

    private native float nativeGetFloat(int rowId, long blockAddress, int columnPosition);

    @Override
    public float getFloat(int rowId) {
        return nativeGetFloat(rowId, blockAddress, columnPosition);
    }

    private native double nativeGetDouble(int rowId, long blockAddress, int columnPosition);

    @Override
    public double getDouble(int rowId) {
        return nativeGetDouble(rowId, blockAddress, columnPosition);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        return null;
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        return null;
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        return null;
    }

    private native String nativeGetString(int rowId, long blockAddress, int columnPosition);

    @Override
    public UTF8String getUTF8String(int rowId) {
        return UTF8String.fromString(nativeGetString(rowId, blockAddress, columnPosition));
    }

    @Override
    public byte[] getBinary(int rowId) {
        return new byte[0];
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        return null;
    }
}
