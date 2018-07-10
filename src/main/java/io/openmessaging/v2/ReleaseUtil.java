package io.openmessaging.v2;

import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

/**
 * Created by Administrator on 2018-07-09.
 */
public class ReleaseUtil {
    public static void releaseMap( MappedByteBuffer buffer){
        try {
            Method m = FileChannelImpl.class.getDeclaredMethod("unmap",
                    MappedByteBuffer.class);
            m.setAccessible(true);
            m.invoke(FileChannelImpl.class, buffer);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }
}
