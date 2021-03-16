package com.hive.function.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * 处理复杂类型
 */
@Description(name = "GenericUDFArrayContains")
public class GenericUDFArrayContains extends GenericUDF {
    private transient ObjectInspector valueOI;
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;
    private BooleanWritable result;


    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentException("The function ARRAY_CONTAINS accepts 2 arguments.");
        }

        if (!(objectInspectors[0].getCategory().equals(ObjectInspector.Category.LIST))) {
            throw new UDFArgumentTypeException(0, "\"array\" expected at function ARRAY_CONTAINS, but \""
                    + objectInspectors[0].getTypeName() + "\" " + "is found");
        }

        this.arrayOI = ((ListObjectInspector) objectInspectors[0]);
        this.arrayElementOI = this.arrayOI.getListElementObjectInspector();

        this.valueOI = objectInspectors[1];

        if (!(ObjectInspectorUtils.compareTypes(this.arrayElementOI, this.valueOI))) {
            throw new UDFArgumentTypeException(1,
                    "\"" + this.arrayElementOI.getTypeName() + "\"" + " expected at function ARRAY_CONTAINS, but "
                            + "\"" + this.valueOI.getTypeName() + "\"" + " is found");
        }

        if (!(ObjectInspectorUtils.compareSupported(this.valueOI))) {
            throw new UDFArgumentException("The function ARRAY_CONTAINS does not support comparison for \""
                    + this.valueOI.getTypeName() + "\"" + " types");
        }

        this.result = new BooleanWritable(false);

        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        this.result.set(false);
        Object array = deferredObjects[0].get();
        Object value = deferredObjects[1].get();

        int arrayLength = this.arrayOI.getListLength(array);

        if ((value == null) || (arrayLength <= 0)) {
            return this.result;
        }

        for (int i = 0; i < arrayLength; ++i) {
            Object listElement = this.arrayOI.getListElement(array, i);
            if ((listElement == null)
                    || (ObjectInspectorUtils.compare(value, this.valueOI, listElement, this.arrayElementOI) != 0))
                continue;
            this.result.set(true);
            break;
        }

        return this.result;
    }

    public String getDisplayString(String[] children) {
        return "array_contains(" + children[0] + ", " + children[1] + ")";
    }
}
