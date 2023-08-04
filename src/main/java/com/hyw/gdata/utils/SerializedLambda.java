package com.hyw.gdata.utils;

import com.hyw.gdata.exception.DbException;
import lombok.Getter;
import lombok.Setter;

import java.io.*;

@Getter
@Setter
public class SerializedLambda implements Serializable {
    private static final long serialVersionUID = 8025925345765570181L;
    private Class<?> capturingClass;
    private String functionalInterfaceClass;
    private String functionalInterfaceMethodName;
    private String functionalInterfaceMethodSignature;
    private String implClass;
    private String implMethodName;
    private String implMethodSignature;
    private int implMethodKind;
    private String instantiatedMethodType;
    private Object[] capturedArgs;

    public SerializedLambda() {
    }

    public static SerializedLambda resolve(QFunction<?, ?> lambda) {
        if (!lambda.getClass().isSynthetic()) {
            throw new DbException("该方法仅能传入 lambda 表达式产生的合成类"+new Object[0]);
        } else {
            try {
                ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(QueryUtil.serialize(lambda))) {
                    protected Class<?> resolveClass(ObjectStreamClass objectStreamClass) throws IOException, ClassNotFoundException {
                        Class clazz;
                        try {
                            clazz = QueryUtil.toClassConfident(objectStreamClass.getName());
                        } catch (Exception var4) {
                            clazz = super.resolveClass(objectStreamClass);
                        }

                        return clazz == java.lang.invoke.SerializedLambda.class ? SerializedLambda.class : clazz;
                    }
                };
                Throwable var2 = null;

                SerializedLambda var3;
                try {
                    var3 = (SerializedLambda)objIn.readObject();
                } catch (Throwable var13) {
                    var2 = var13;
                    throw var13;
                } finally {
                    if (objIn != null) {
                        if (var2 != null) {
                            try {
                                objIn.close();
                            } catch (Throwable var12) {
                                var2.addSuppressed(var12);
                            }
                        } else {
                            objIn.close();
                        }
                    }

                }

                return var3;
            } catch (IOException | ClassNotFoundException var15) {
                throw new DbException("This is impossible to happen "+ var15 +"  "+new Object[0]);
            }
        }
    }

    public String getFunctionalInterfaceClassName() {
        return this.normalizedName(this.functionalInterfaceClass);
    }

    public Class<?> getImplClass() {
        return QueryUtil.toClassConfident(this.getImplClassName());
    }

    public String getImplClassName() {
        return this.normalizedName(this.implClass);
    }

    public String getImplMethodName() {
        return this.implMethodName;
    }

    private String normalizedName(String name) {
        return name.replace('/', '.');
    }

    public Class<?> getInstantiatedType() {
        String instantiatedTypeName = this.normalizedName(this.instantiatedMethodType.substring(2, this.instantiatedMethodType.indexOf(59)));
        return QueryUtil.toClassConfident(instantiatedTypeName);
    }

    public String toString() {
        String interfaceName = this.getFunctionalInterfaceClassName();
        String implName = this.getImplClassName();
        return String.format("%s -> %s::%s", interfaceName.substring(interfaceName.lastIndexOf(46) + 1), implName.substring(implName.lastIndexOf(46) + 1), this.implMethodName);
    }
}
