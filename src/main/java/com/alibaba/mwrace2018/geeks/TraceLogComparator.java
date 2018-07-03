package com.alibaba.mwrace2018.geeks;

import java.util.Comparator;

/**
 * @author 奥陌
 */
public class TraceLogComparator implements Comparator<TraceLog> {

    @Override
    public int compare(TraceLog o1, TraceLog o2) {
        if (o1 == null || o1.getRpcId() == null) {
            return -1;
        } else if (o2 == null || o2.getRpcId() == null) {
            return 1;
        }
        if (o1.getRpcId().equals(o2.getRpcId())) {
            return o1.getRpcType() - o2.getRpcType();
        }

        int [] array0 = getRpcIdArray(o1.getRpcId());
        int [] array1 = getRpcIdArray(o2.getRpcId());
        int size = Math.min(array0.length, array1.length);
        for (int i = 0; i < size; i++) {
            int result = array0[i] - array1[i];
            if (result != 0) {
                return result;
            }
        }
        return array0.length - array1.length;
    }

    private static int[] getRpcIdArray(String rpcId) {
        if (rpcId != null) {
            String[] strs = rpcId.split("\\.");
            int[] ints = new int[strs.length];
            for (int i = 0; i < strs.length; ++i) {
                ints[i] = Integer.parseInt(strs[i]);
            }
            return ints;
        }
        return new int[] { 0 };
    }

}
