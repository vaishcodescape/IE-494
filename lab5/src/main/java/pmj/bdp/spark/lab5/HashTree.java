package pmj.bdp.spark.lab5;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Apriori hash tree used by YAFIM Algo-3 {@code subset()} to find which
 * candidate k-itemsets a transaction contains (Qiu et al., IPDPSW 2014).
 *
 * Interior nodes hash on the next item; leaves store candidates. A leaf
 * splits once it exceeds {@link #MAX_LEAF} and depth &lt; k.
 */
public final class HashTree implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int BRANCH = 11;
    private static final int MAX_LEAF = 4;

    private final Node root;
    private final int k;

    public HashTree(List<int[]> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            throw new IllegalArgumentException("candidates must be non-empty");
        }
        this.k = candidates.get(0).length;
        this.root = new Node(true);
        for (int[] c : candidates) {
            insert(root, c, 0);
        }
    }

    /**
     * Candidates contained in the (sorted, unique) transaction.
     * Each candidate is returned at most once: hash collisions can visit the
     * same leaf via multiple item prefixes, which would otherwise inflate support.
     */
    public List<int[]> findCandidates(int[] transaction) {
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        List<int[]> hits = new ArrayList<>();
        search(root, transaction, 0, 0, seen, hits);
        return hits;
    }

    private void insert(Node node, int[] itemset, int depth) {
        if (node.leaf) {
            node.candidates.add(itemset);
            if (node.candidates.size() > MAX_LEAF && depth < k) {
                split(node, depth);
            }
            return;
        }
        int bucket = bucket(itemset[depth]);
        if (node.children[bucket] == null) {
            node.children[bucket] = new Node(true);
        }
        insert(node.children[bucket], itemset, depth + 1);
    }

    private void split(Node leaf, int depth) {
        List<int[]> old = leaf.candidates;
        leaf.leaf = false;
        leaf.candidates = null;
        leaf.children = new Node[BRANCH];
        for (int[] c : old) {
            insert(leaf, c, depth);
        }
    }

    private void search(Node node, int[] t, int depth, int start,
                        LinkedHashSet<String> seen, List<int[]> hits) {
        if (node == null) {
            return;
        }
        if (node.leaf) {
            for (int[] cand : node.candidates) {
                if (Itemset.containsAll(t, cand) && seen.add(Itemset.toKey(cand))) {
                    hits.add(cand);
                }
            }
            return;
        }
        int remaining = k - depth;
        for (int i = start; i <= t.length - remaining; i++) {
            Node child = node.children[bucket(t[i])];
            if (child != null) {
                search(child, t, depth + 1, i + 1, seen, hits);
            }
        }
    }

    private static int bucket(int item) {
        int m = item % BRANCH;
        return m < 0 ? m + BRANCH : m;
    }

    private static final class Node implements Serializable {
        private static final long serialVersionUID = 1L;
        boolean leaf;
        List<int[]> candidates;
        Node[] children;

        Node(boolean leaf) {
            this.leaf = leaf;
            if (leaf) {
                this.candidates = new ArrayList<>();
            } else {
                this.children = new Node[BRANCH];
            }
        }
    }
}
