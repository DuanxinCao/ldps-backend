package io.glutenproject.ldps.backend.jni;


public class BatchIterator {
    private native boolean nativeHasNext(long nativeHandle);

    private native byte[] nativeNext(long nativeHandle);

    private native long nativeCHNext(long nativeHandle);

    private native void nativeClose(long nativeHandle);

    private native Object nativeFetchMetrics(long nativeHandle);
}
