package com.test.LoadBalancerDemo.streamProcessor;

import com.test.LoadBalancerDemo.streamProcessor.boltDetails.BottleneckDetails;

import com.google.common.collect.Multimap;
import com.google.common.collect.Lists;


import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class BottleneckSorter {
  /*  private static int causePriority(String cause) {
        if (cause.toLowerCase().contains("cpu")) {
            return 2; // Higher priority
        } else {
            return 1; // Lower priority (BW)
        }
    }


     * Sort the multimap entries by:
     *   1) CPU cause before BW cause
     *   2) Descending usage percentage within the same cause

    public static List<Map.Entry<String, BottleneckDetails>> sortByPriority(
            Multimap<String, BottleneckDetails> listOfVmsWithBottleNeck) {

        // Flatten the Multimap into a list of (VM, BottleneckDetails) entries
        List<Map.Entry<String, BottleneckDetails>> entryList =
                Lists.newArrayList(listOfVmsWithBottleNeck.entries());

        // Sort using a custom comparator
        entryList.sort(new Comparator<Map.Entry<String, BottleneckDetails>>() {
            @Override
            public int compare(Map.Entry<String, BottleneckDetails> e1,
                               Map.Entry<String, BottleneckDetails> e2) {

                // 1) Compare by cause priority
                int p1 = causePriority(e1.getValue().getCause());
                int p2 = causePriority(e2.getValue().getCause());
                if (p1 != p2) {
                    // Descending priority: higher priority should come first
                    return Integer.compare(p2, p1);
                }

                // 2) If cause priority is the same, compare usage % descending
                double usage1 = e1.getValue().getUsagePercentage();
                double usage2 = e2.getValue().getUsagePercentage();
                return Double.compare(usage2, usage1);
            }
        });

        return entryList;
    }
   */

    public static List<Map.Entry<String, BottleneckDetails>> sortByPriority(
            Multimap<String, BottleneckDetails> listOfVmsWithBottleNeck) {

        List<Map.Entry<String, BottleneckDetails>> entryList =
                Lists.newArrayList(listOfVmsWithBottleNeck.entries());

        entryList.sort(new Comparator<Map.Entry<String, BottleneckDetails>>() {
            @Override
            public int compare(Map.Entry<String, BottleneckDetails> e1,
                               Map.Entry<String, BottleneckDetails> e2) {
                // Compare usage percentages in descending order
                double usage1 = e1.getValue().getUsagePercentage();
                double usage2 = e2.getValue().getUsagePercentage();
                return Double.compare(usage2, usage1); // Higher first
            }
        });

        return entryList;
    }
}
