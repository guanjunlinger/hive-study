package com.hive.function.udtf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;


public class UDTFFor extends GenericUDTF {

    private IntWritable start;
    private IntWritable end;
    private IntWritable inc;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) {
        start = ((WritableConstantIntObjectInspector) argOIs[0]).getWritableConstantValue();
        end = ((WritableConstantIntObjectInspector) argOIs[1]).getWritableConstantValue();
        if (argOIs.length == 3) {
            inc = ((WritableConstantIntObjectInspector) argOIs[1]).getWritableConstantValue();
        } else {
            inc = new IntWritable(1);
        }
        List<String> fieldNames = new ArrayList();
        fieldNames.add("col_0");
        List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();
        objectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT));
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, objectInspectors);
    }

    public void process(Object[] objects) throws HiveException {
        for (int i = start.get(); i < end.get(); i += inc.get()) {
            forward(new Object[]{i});
        }


    }

    public void close() {
    }
}
