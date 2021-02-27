/**
 * Sort Merge Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

public class SortMergeJoin extends Join {
    int batchsize;                          // Number of tuples per out batch
    ArrayList<Attribute> leftattribute;     // Join attributes in left table
    ArrayList<Attribute> rightattribute;    // Join attributes in right table
    ArrayList<Integer> leftindex;           // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;          // Indices of the join attributes in right table
    Batch leftbatch;                        // Buffer page for left input stream
    Batch rightbatch;                       // Buffer page for right input stream
    Batch outbatch;                         // Buffer page for output

    Sort lsort;                             // Sort left table
    Sort rsort;                             // Sort right table
    Tuple lefttuple = null;                 // Current tuple from left buffer page
    Tuple righttuple = null;                // Current tuple from right buffer page
    Tuple prevtuple = null;                 // Current tuple from left buffer page

    int lcurs = 0;                          // Cursor for left block
    int rcurs = 0;                          // Cursor for right block
    boolean eosl = false;                   // Whether end of stream (left table) is reached
    boolean eosr = false;                   // Whether end of stream (right table) is reached

    ArrayList<Tuple> leftpartition = new ArrayList<>();
    ArrayList<Tuple> rightpartition = new ArrayList<>();
    int lpcurs = 0;
    int rpcurs = 0;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        int tupleSize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tupleSize;

        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        leftattribute = new ArrayList<>();
        rightattribute = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftattribute.add(leftattr);
            rightattribute.add(rightattr);
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        if (!left.open()) return false;
        if (!right.open()) return false;

        lsort = new Sort(left, leftattribute, numBuff, true);
        rsort = new Sort(right, rightattribute, numBuff, true);

        return lsort.open() && rsort.open();
    }

    public Batch next() {
        if (eosl || eosr) {
            close();
            return null;
        }

        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
            if (lpcurs == 0 && rpcurs == 0) {
                if (lefttuple == null) {
                    leftbatch = (Batch) lsort.next();
                    if (leftbatch == null || leftbatch.isEmpty()) {
                        eosl = true;
                        return outbatch;
                    }
                    lefttuple = leftbatch.get(lcurs);
                }
                if (righttuple == null) {
                    rightbatch = (Batch) rsort.next();
                    if (rightbatch == null || rightbatch.isEmpty()) {
                        eosr = true;
                        return outbatch;
                    }
                    righttuple = rightbatch.get(rcurs);
                }

                while (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) != 0) {
                    while (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) < 0) {
                        lcurs++;
                        if (lcurs == leftbatch.size()) {
                            lcurs = 0;
                            leftbatch = (Batch) lsort.next();
                            if (leftbatch == null || leftbatch.isEmpty()) {
                                eosl = true;
                                return outbatch;
                            }
                        }
                        lefttuple = leftbatch.get(lcurs);
                    }
                    while (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) > 0) {
                        rcurs++;
                        if (rcurs == rightbatch.size()) {
                            rcurs = 0;
                            rightbatch = (Batch) rsort.next();
                            if (rightbatch == null || rightbatch.isEmpty()) {
                                eosr = true;
                                return outbatch;
                            }
                        }
                        righttuple = rightbatch.get(rcurs);
                    }
                }

                prevtuple = lefttuple;

                while (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) == 0) {
                    leftpartition.add(lefttuple);
                    lcurs++;
                    if (lcurs == leftbatch.size()) {
                        lcurs = 0;
                        leftbatch = (Batch) lsort.next();
                        if (leftbatch == null || leftbatch.isEmpty()) {
                            eosl = true;
                            break;
                        }
                    }
                    lefttuple = leftbatch.get(lcurs);
                }
                while (Tuple.compareTuples(prevtuple, righttuple, leftindex, rightindex) == 0) {
                    rightpartition.add(righttuple);
                    rcurs++;
                    if (rcurs == rightbatch.size()) {
                        rcurs = 0;
                        rightbatch = (Batch) rsort.next();
                        if (rightbatch == null || rightbatch.isEmpty()) {
                            eosr = true;
                            break;
                        }
                    }
                    righttuple = rightbatch.get(rcurs);
                }
            }

            for (int i = lpcurs; i < leftpartition.size(); i++) {
                for (int j = rpcurs; j < rightpartition.size(); j++) {
                    Tuple ltuple = leftpartition.get(i);
                    Tuple rtuple = rightpartition.get(j);
                    if (ltuple.checkJoin(rtuple, leftindex, rightindex)) {
                        Tuple outtuple = ltuple.joinWith(rtuple);
                        outbatch.add(outtuple);
                    }
                    if (outbatch.isFull()) {
                        if (i == leftpartition.size() - 1 && j == rightpartition.size() - 1) {
                            lpcurs = 0;
                            rpcurs = 0;
                            leftpartition.clear();
                            rightpartition.clear();
                        } else if (i != leftpartition.size() - 1 && j == rightpartition.size() - 1) {
                            lpcurs = i + 1;
                            rpcurs = 0;
                        } else if (i == leftpartition.size() - 1 && j != rightpartition.size() - 1) {
                            lpcurs = i;
                            rpcurs = j + 1;
                        } else {
                            lpcurs = i;
                            rpcurs = j + 1;
                        }
                        return outbatch;
                    }
                }
                rpcurs = 0;
            }
            lpcurs = 0;
            leftpartition.clear();
            rightpartition.clear();

            if (eosl || eosr) {
                return outbatch;
            }
        }

        return outbatch;
    }

    public boolean close() {
        lsort.close();
        rsort.close();
        return true;
    }
}
