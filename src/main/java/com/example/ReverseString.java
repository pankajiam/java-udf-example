package com.example;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ReverseString extends GenericUDF {
    static final Log LOG = LogFactory.getLog(ReverseString.class);
    private StringObjectInspector input;
    private String myConfigVar = "Default Value";
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1)
            throw new UDFArgumentException("input must have length 1");
        ObjectInspector input = objectInspectors[0];
        if (!(input instanceof StringObjectInspector))
            throw new UDFArgumentException("input must be a string");
        this.input = (StringObjectInspector) input;
        System.out.println("Success. Input formatted correctly");
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    public Text evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String s = input.getPrimitiveJavaObject(deferredObjects[0].get());
        return new Text(reverseString(s));
    }

    public String getDisplayString(String[] strings) {
        return "UDF example to reverse string";
    }

    private String reverseString(String s) {
        byte [] strAsByteArray = s.getBytes();
        byte [] result = new byte [strAsByteArray.length];
        for (int i = 0; i < strAsByteArray.length; i++)
            result[i] = strAsByteArray[strAsByteArray.length-i-1];
        return new String(result);
    }

    // this will require if you want read hive configuration variable
    @Override
    public void configure(MapredContext context) {
        JobConf conf = context.getJobConf();
        this.myConfigVar = conf.get("MY_CONFIG_VALUE");

        if (this.myConfigVar == null) {
            LOG.error("Configuration variable MY_CONFIG_VALUE is null");
            System.exit(-1);
        }
    }
}
