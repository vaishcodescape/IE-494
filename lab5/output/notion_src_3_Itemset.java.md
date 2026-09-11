<details>
<summary>Itemset.java (full)</summary>
	```java
	package pmj.bdp.spark.lab5;
	
	import java.util.ArrayList;
	import java.util.Arrays;
	import java.util.HashSet;
	import java.util.List;
	import java.util.Set;
	
	/**
	 * Sorted integer itemset helpers (Apriori join + prune).
	 */
	public final class Itemset {
	
	    private Itemset() {
	    }
	
	    public static int[] parseTransaction(String line) {
	        if (line == null || line.isBlank()) {
	            return new int[0];
	        }
	        String[] tok = line.trim().split("\\s+");
	        int[] raw = new int[tok.length];
	        int n = 0;
	        for (String t : tok) {
	            if (t.isEmpty()) {
	                continue;
	            }
	            raw[n++] = Integer.parseInt(t);
	        }
	        if (n == 0) {
	            return new int[0];
	        }
	        Arrays.sort(raw, 0, n);
	        int w = 1;
	        for (int i = 1; i < n; i++) {
	            if (raw[i] != raw[w - 1]) {
	                raw[w++] = raw[i];
	            }
	        }
	        return Arrays.copyOf(raw, w);
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
	
	    public static int[] parseKey(String key) {
	        String[] parts = key.split(",");
	        int[] items = new int[parts.length];
	        for (int i = 0; i < parts.length; i++) {
	            items[i] = Integer.parseInt(parts[i].trim());
	        }
	        return items;
	    }
	
	    /** Both arrays sorted. */
	    public static boolean containsAll(int[] transaction, int[] candidate) {
	        int i = 0;
	        int j = 0;
	        while (i < transaction.length && j < candidate.length) {
	            if (transaction[i] == candidate[j]) {
	                i++;
	                j++;
	            } else if (transaction[i] < candidate[j]) {
	                i++;
	            } else {
	                return false;
	            }
	        }
	        return j == candidate.length;
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
	```
</details>