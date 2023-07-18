package com.ldps.examples;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Test1 {
    public static void test1() throws Exception {
        String sql = "SELECT id, name FROM default.numbers";
        CHNativeExpressionEvaluator chNativeExpressionEvaluator = new CHNativeExpressionEvaluator();
        List<GeneralInIterator> iterList = new ArrayList<>();
        GeneralOutIterator kernelWithBatchIterator = chNativeExpressionEvaluator.createKernelWithBatchIterator(sql, iterList, null);
        ColumnarBatch columnarBatch = kernelWithBatchIterator.nextInternal();
        Iterator<InternalRow> internalRowIterator = columnarBatch.rowIterator();
        while(internalRowIterator.hasNext()){
            System.out.println("--------");
            InternalRow next = internalRowIterator.next();
            System.out.println(next.getLong(0));
            System.out.println(next.getString(1));
        }
    }

    public static void test2() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS default.test2 (id UInt64, name String) ENGINE = Memory";
        CHNativeExpressionEvaluator chNativeExpressionEvaluator = new CHNativeExpressionEvaluator();
        List<GeneralInIterator> iterList = new ArrayList<>();
        GeneralOutIterator kernelWithBatchIterator = chNativeExpressionEvaluator.createKernelWithBatchIterator(sql, iterList, null);
    }


    public static void main(String[] args) throws Exception {
        System.loadLibrary("ldpsbackend");



        Query query = new Query();
        query.sqlQuery("SELECT id, name FROM default.numbers");
        Test1.test1();
//        test2();
    }
}
