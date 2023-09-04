package io.glutenproject.ldps.backend;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Test1 {
    public static void test1() throws Exception {
        String sql = "SELECT id, name FROM default.numbers";
        CHNativeExpressionEvaluator chNativeExpressionEvaluator = new CHNativeExpressionEvaluator();
        List<GeneralInIterator> iterList = new ArrayList<>();
        GeneralOutIterator kernelWithBatchIterator = chNativeExpressionEvaluator.createKernelWithBatchIterator(sql, iterList, null);
        while (kernelWithBatchIterator.hasNext()) {
            System.out.println("++++++");
            ColumnarBatch columnarBatch = kernelWithBatchIterator.next();
            Iterator<InternalRow> internalRowIterator = columnarBatch.rowIterator();
            while (internalRowIterator.hasNext()) {
                InternalRow next = internalRowIterator.next();
                System.out.println(next.getLong(0));
                System.out.println(next.getString(1));
            }
        }

    }

    public static void test2() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS default.test2 (id UInt64, name String) ENGINE = Memory";
        CHNativeExpressionEvaluator chNativeExpressionEvaluator = new CHNativeExpressionEvaluator();
        List<GeneralInIterator> iterList = new ArrayList<>();
        GeneralOutIterator kernelWithBatchIterator = chNativeExpressionEvaluator.createKernelWithBatchIterator(sql, iterList, null);
    }


    public static void main(String[] args) throws Exception {
//        lsof -nP -p 21528 | grep LISTEN
        System.loadLibrary("ldpsbackend");
        System.loadLibrary("ldpsbackend");
        System.loadLibrary("ldpsbackend");

//        Query query = new Query();
//        query.sqlQuery("SELECT id, name FROM default.numbers");
        Test1.test1();
//        test2();
    }
}