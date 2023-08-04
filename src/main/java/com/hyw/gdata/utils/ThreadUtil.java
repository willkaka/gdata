package com.hyw.gdata.utils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ThreadUtil {


    /**
     * 获取登记错误日志的类名
     * @return
     */
    public static String getLastCallClassName() {
        try {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            if (stackTrace == null || stackTrace.length <= 0) {
                return "BatchLogServiceErr003:无法获取调用者类名和方法名！";
            }

            StackTraceElement log = stackTrace[1];
            if (log == null) {
                return "BatchLogServiceErr004:无法获取调用者类名和方法名！";
            }

            String tag = null;
            for (int i = 1; i < stackTrace.length; i++) {
                StackTraceElement e = stackTrace[i];
                if (!e.getClassName().equals(log.getClassName())) {
                    tag = e.getClassName() + "." + e.getMethodName() + "()";
                    break;
                }
            }
            if (tag == null) {
                tag = log.getClassName() + "." + log.getMethodName();
            }
            return tag;
        } catch (Exception e) {
            return "BatchLogServiceErr005:获取调用者类名和方法名出错！";
        }

    }


    /**
     * 获取登记错误日志的类名
     * @return
     */
    public static Method getLastCallMethod(StackTraceElement[] stack) {
        try {
//            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            if (stack == null || stack.length <= 0) {
                //"BatchLogServiceErr003:无法获取调用者类名和方法名！";
                return null;
            }

            StackTraceElement parent = stack[1];
            if (parent == null) {
                // "BatchLogServiceErr004:无法获取调用者类名和方法名！";
                return null;
            }
            return getMethodFrom(parent);
        } catch (Exception e) {
            // "BatchLogServiceErr005:获取调用者类名和方法名出错！";
            return null;
        }
    }

    /**
     * 获取登记错误日志的类名
     * @return
     */
    public static List<Method> getLastCallMethodList(StackTraceElement[] stack) {
        List<Method> methodList = new ArrayList<>();
        try {
//            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            if (stack == null || stack.length <= 0) {
                //"BatchLogServiceErr003:无法获取调用者类名和方法名！";
                return null;
            }

            for(int i=1;i<stack.length;i++) {
                StackTraceElement parent = stack[i];
                if (parent == null) {
                    // "BatchLogServiceErr004:无法获取调用者类名和方法名！";
                    break;
                }
                Method method = getMethodFrom(parent);
                if(method!=null) methodList.add(method);
            }
        } catch (Exception e) {
            // "BatchLogServiceErr005:获取调用者类名和方法名出错！";
        }
        return methodList;
    }

    private static Method getMethodFrom(StackTraceElement parent){
        try {
            Class cls = Class.forName(parent.getClassName());
            String methodName = parent.getMethodName();
//            ClassPool pool = ClassPool.getDefault();
//            ClassClassPath classPath = new ClassClassPath(cls);
//            pool.insertClassPath(classPath);
//            CtClass cc = pool.get(cls.getName());
//            Class[] paramsTypes = null;
//            for (CtMethod ctMethod : cc.getMethods()) {
//                MethodInfo methodInfo = ctMethod.getMethodInfo();
//                if (!methodName.equals(methodInfo.getName())) {
//                    continue;
//                }
//                //取得方法起始行和结束行，然后对比执行的代码是否在此方法区间内
//                int ctMethodMaxLine = methodInfo.getLineNumber(Integer.MAX_VALUE);
//                int ctMethodMinLine = methodInfo.getLineNumber(0);
//                int methodLine = parent.getLineNumber();
//                if (!(methodLine <= ctMethodMaxLine && methodLine >= ctMethodMinLine)) {
//                    continue;
//                }
//                CtClass[] ctClass = ctMethod.getParameterTypes();
//                paramsTypes = new Class[ctMethod.getParameterTypes().length];
//                for (int i = 0; i < ctClass.length; i++) {
//                    CtClass paramType = ctClass[i];
//                    Class type = null;
//                    String clazzName = null;
//                    if (paramType.isPrimitive()) {
//                        type = ClassUtils.forName(paramType.getSimpleName(), Thread.currentThread().getContextClassLoader());
//                    } else {
//                        type = ClassUtils.forName((paramType.getPackageName() + "." + paramType.getSimpleName()), Thread.currentThread().getContextClassLoader());
//                    }
//                    paramsTypes[i] = type;
//                }
//                break;
//            }
//            return ReflectionUtils.findMethod(cls, methodName, paramsTypes);
            return null;
        }catch (Exception e){
            // "BatchLogServiceErr005:获取调用者类名和方法名出错！";
            return null;
        }
    }
}
