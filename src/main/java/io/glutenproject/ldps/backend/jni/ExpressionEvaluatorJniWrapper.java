package io.glutenproject.ldps.backend.jni;



public class ExpressionEvaluatorJniWrapper {

    /**
     * Call initNative to initialize native computing.
     */
    native void nativeInitNative(byte[] confAsPlan);

    /**
     * Call finalizeNative to finalize native computing.
     */
    native void nativeFinalizeNative();

    /**
     * Validate the Substrait plan in native compute engine.
     *
     * @param subPlan the Substrait plan in binary format.
     * @return whether the computing of this plan is supported in native.
     */
    native boolean nativeDoValidate(byte[] subPlan);

    /**
     * Create a native compute kernel and return a columnar result iterator.
     *
     * @param allocatorId allocator id
     * @return iterator instance id
     */
    public native long nativeCreateKernelWithIterator(
            long allocatorId,
            byte[] wsPlan,
            Object[] batchItr) throws RuntimeException;

    /**
     * Create a native compute kernel and return a row iterator.
     */
    native long nativeCreateKernelWithRowIterator(byte[] wsPlan) throws RuntimeException;

    /**
     * Closes the projector referenced by nativeHandler.
     *
     * @param nativeHandler nativeHandler that needs to be closed
     */
    native void nativeClose(long nativeHandler);
}
