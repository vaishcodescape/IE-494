package pmj.bdp.mr.lab3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sorted integer itemset helpers for MR-Apriori.
 */
public final class Itemset {

    private Itemset() {
    }

    public static int[] parse(String text) {
        String[] parts = text.trim().split(",");
        int[] items = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            items[i] = Integer.parseInt(parts[i].trim());
        }
        Arrays.sort(items);
        return items;
    }

    public static String toKey(int[] items) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < items.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(items[i]);
        }
        return sb.toString();
    }

    public static boolean containsAll(Set<Integer> transaction, int[] candidate) {
        for (int item : candidate) {
            if (!transaction.contains(item)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Apriori-gen: self-join L_{k-1} then prune candidates whose
     * (k-1)-subsets are not all frequent.
     */
    public static List<int[]> generateCandidates(List<int[]> previous) {
        List<int[]> candidates = new ArrayList<>();
        if (previous.isEmpty()) {
            return candidates;
        }
        int kMinus1 = previous.get(0).length;
        Set<String> frequent = new HashSet<>();
        for (int[] itemset : previous) {
            frequent.add(toKey(itemset));
        }

        for (int i = 0; i < previous.size(); i++) {
            int[] a = previous.get(i);
            for (int j = i + 1; j < previous.size(); j++) {
                int[] b = previous.get(j);
                if (!prefixEqual(a, b, kMinus1 - 1)) {
                    continue;
                }
                // last items differ; keep them sorted
                int lastA = a[kMinus1 - 1];
                int lastB = b[kMinus1 - 1];
                if (lastA == lastB) {
                    continue;
                }
                int[] cand = new int[kMinus1 + 1];
                System.arraycopy(a, 0, cand, 0, kMinus1 - 1);
                if (lastA < lastB) {
                    cand[kMinus1 - 1] = lastA;
                    cand[kMinus1] = lastB;
                } else {
                    cand[kMinus1 - 1] = lastB;
                    cand[kMinus1] = lastA;
                }
                if (allSubsetsFrequent(cand, frequent)) {
                    candidates.add(cand);
                }
            }
        }
        return candidates;
    }

    private static boolean prefixEqual(int[] a, int[] b, int len) {
        for (int i = 0; i < len; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean allSubsetsFrequent(int[] candidate, Set<String> frequent) {
        int k = candidate.length;
        int[] subset = new int[k - 1];
        for (int drop = 0; drop < k; drop++) {
            int idx = 0;
            for (int i = 0; i < k; i++) {
                if (i != drop) {
                    subset[idx++] = candidate[i];
                }
            }
            if (!frequent.contains(toKey(subset))) {
                return false;
            }
        }
        return true;
    }
}
