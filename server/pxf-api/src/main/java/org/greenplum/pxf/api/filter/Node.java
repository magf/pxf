package org.greenplum.pxf.api.filter;

import lombok.Getter;
import lombok.Setter;

/**
 * A node in the expression tree
 */
@Setter
@Getter
public class Node {
    private Node left;
    private Node right;

    /**
     * Default constructor
     */
    public Node() {
        this(null, null);
    }

    /**
     * Constructs a node with a left Node
     *
     * @param left the left node
     */
    public Node(Node left) {
        this(left, null);
    }

    /**
     * Constructs a node with a left and right node
     *
     * @param left  the left node
     * @param right the right node
     */
    public Node(Node left, Node right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Returns the number of children for this node
     *
     * @return the number of children for this node
     */
    public int childCount() {
        int count = 0;
        if (left != null) count++;
        if (right != null) count++;
        return count;
    }
}
