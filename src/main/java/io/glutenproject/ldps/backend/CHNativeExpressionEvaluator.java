package io.glutenproject.ldps.backend;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.expressions.Attribute;

import java.io.IOException;
import java.util.List;

public class CHNativeExpressionEvaluator {
    private final ExpressionEvaluatorJniWrapper jniWrapper;

    public CHNativeExpressionEvaluator() {
        jniWrapper = new ExpressionEvaluatorJniWrapper();
    }

    // Used to initialize the native computing.
    public void initNative(SparkConf conf) {
//        Tuple2<String, String>[] all = conf.getAll();
//        Map<String, String> confMap =
//                Arrays.stream(all).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
//        String prefix = BackendsApiManager.getSettings().getBackendConfigPrefix();
//        Map<String, String> nativeConfMap =
//                GlutenConfig.getNativeBackendConf(prefix, JavaConverters.mapAsScalaMap(confMap));
//
//        // Get the customer config from SparkConf for each backend
//        BackendsApiManager.getTransformerApiInstance().postProcessNativeConfig(nativeConfMap, prefix);
//
//        jniWrapper.nativeInitNative(buildNativeConfNode(nativeConfMap).toProtobuf().toByteArray());
    }

    public void finalizeNative() {
//        jniWrapper.nativeFinalizeNative();
    }

    // Used to validate the Substrait plan in native compute engine.
    public boolean doValidate(byte[] subPlan) {
//        return jniWrapper.nativeDoValidate(subPlan);
        return false;
    }

//    private PlanNode buildNativeConfNode(Map<String, String> confs) {
//        StringMapNode stringMapNode = ExpressionBuilder.makeStringMap(confs);
//        AdvancedExtensionNode extensionNode = ExtensionBuilder
//                .makeAdvancedExtension(Any.pack(stringMapNode.toProtobuf()));
//        return PlanBuilder.makePlan(extensionNode);
//    }

    // Used by WholeStageTransform to create the native computing pipeline and
    // return a columnar result iterator.
    public GeneralOutIterator createKernelWithBatchIterator(
            String wsPlan, List<GeneralInIterator> iterList, List<Attribute> outAttrs)
            throws RuntimeException, IOException {
        //TODO memeory manager
//        long allocId = CHNativeMemoryAllocators.contextInstance().getNativeInstanceId();
        long allocId = 0L;
        long handle =
                jniWrapper.nativeCreateKernelWithIterator(allocId, getPlanBytesBuf(wsPlan),
                        iterList.toArray(new GeneralInIterator[0]));
        return createOutIterator(handle, outAttrs);
    }

    // Only for UT.
    public GeneralOutIterator createKernelWithBatchIterator(
            long allocId,
            byte[] wsPlan, List<GeneralInIterator> iterList, List<Attribute> outAttrs)
            throws RuntimeException, IOException {
        long handle =
                jniWrapper.nativeCreateKernelWithIterator(allocId, wsPlan,
                        iterList.toArray(new GeneralInIterator[0]));
        return createOutIterator(handle, outAttrs);
    }

    private byte[] getPlanBytesBuf(String plan) {
//        return planNode.toByteArray();
        return plan.getBytes();
    }

    private GeneralOutIterator createOutIterator(
            long nativeHandle, List<Attribute> outAttrs) throws IOException {
        return new BatchIterator(nativeHandle, outAttrs);
    }
}
