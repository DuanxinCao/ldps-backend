package io.glutenproject.ldps.backend.jni;

public class CHNativeBlock {
    private native int nativeNumRows(long blockAddress);
    private native int nativeNumColumns(long blockAddress);
    private native byte[] nativeColumnType(long blockAddress, int position);
    private native long nativeTotalBytes(long blockAddress);
    public native void nativeClose(long blockAddress);

}
