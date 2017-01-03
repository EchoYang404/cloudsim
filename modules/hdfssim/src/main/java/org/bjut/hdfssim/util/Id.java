package org.bjut.hdfssim.util;
import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.HFile;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.cloudbus.cloudsim.*;

public final class Id {

    private static final Map<Class<?>, Integer> COUNTERS = new LinkedHashMap<>();
    private static final Set<Class<?>> NO_COUNTERS = new HashSet<>();
    private static int globalCounter = 1;

    static {
        COUNTERS.put(Cloudlet.class, 1);
        COUNTERS.put(Vm.class, 1);
        COUNTERS.put(Host.class, 1);
        COUNTERS.put(DatacenterBroker.class, 1);
        COUNTERS.put(Pe.class, 1);


        COUNTERS.put(Block.class,1);
        COUNTERS.put(Storage.class,1);
        COUNTERS.put(HFile.class,1);
        COUNTERS.put(Datanode.class,1);
    }

    private Id() {
    }

    /**
     * Returns a valid id for the specified class.
     *
     * @param clazz
     *            - the class of the object to get an id for. Must not be null.
     * @return a valid id for the specified class.
     */
    public static synchronized int pollId(final Class<?> clazz) {
        Class<?> matchClass = null;
        if (COUNTERS.containsKey(clazz)) {
            matchClass = clazz;
        } else if (!NO_COUNTERS.contains(clazz)) {
            for (Class<?> key : COUNTERS.keySet()) {
                if (key.isAssignableFrom(clazz)) {
                    matchClass = key;
                    break;
                }
            }
        }

        int result;
        if (matchClass == null) {
            NO_COUNTERS.add(clazz);
            result = pollGlobalId();
        } else {
            result = COUNTERS.get(matchClass);
            COUNTERS.put(matchClass, result + 1);
        }

        if (result < 0) {
            throw new IllegalStateException("The generated id for class:" + clazz.getName()
                    + " is negative. Possible integer overflow.");
        }

        return result;
    }

    private static synchronized int pollGlobalId() {
        return globalCounter++;
    }

    public static synchronized int getId(final Class<?> clazz)
    {
        return COUNTERS.get(clazz);
    }
}
