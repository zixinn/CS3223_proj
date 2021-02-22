/**
 * To remove duplicates
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;


public class Distinct extends Operator {

    Operator base;                  // Base Operator
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Attribute> attrset;   // Set of attributes to project
    ArrayList<Integer> attrIndex;   // Indexes of atttributes to sort
    int numBuff;                    // Number of buffers available
    Batch outbatch;                 // Buffer for output
    Batch[] inBufferPages;          // Buffer pages for merging
    SortedRun sortedRun;

    Tuple prevAddedTuple, nextTupleToAdd; // For comparing and merging
    int curs;                       // cursor for input buffer pages
    boolean eos;                    // Whether end of stream is reached

    public Distinct(Operator base, ArrayList<Attribute> as) {
        super(OpType.DISTINCT);
        this.base = base;
        this.attrset = as;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * sorted based on from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        eos = false;
        prevAddedTuple = null;
        nextTupleToAdd = null;
        curs = 0;
        inBufferPages = new Batch[numBuff-1];

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();

        attrIndex = new ArrayList<>(attrset.size());
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggragation is not implemented.");
                System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex.add(index);
        }

        sortedRun = new SortedRun(base, attrIndex, numBuff, true);
        boolean generatedSortedRuns = sortedRun.open();

        // Load the sorted runs into the input buffers
        for (int i = 0; i < numBuff-1; i++) {
            Batch nextBatch = sortedRun.next();
            inBufferPages[i] = nextBatch;
            if (nextBatch == null) {
                break;
            }
        }

        return generatedSortedRuns;
    }

    public Batch next() {
        outbatch = new Batch(batchsize);
        if (eos) {
            return null;
        }
        while (!outbatch.isFull()) {
            nextTupleToAdd = null;
            int page = -1; // page that we took the nextTupleToAdd from
            // Find the next tuple to add
            for (int i = 0; i < numBuff - 2; i++) {
                //System.out.println("i: " + i);
                if (inBufferPages[i] == null && inBufferPages[i+1] == null) {
                    System.out.println("Both buffer pages are null");
                    continue;
                } else if (inBufferPages[i] == null && compareTuples(inBufferPages[i+1].get(0), nextTupleToAdd) < 0) {
                    System.out.println("page " + i + " is null");
                    nextTupleToAdd = inBufferPages[i+1].get(0);
                    page = i+1;
                } else if (inBufferPages[i+1] == null && compareTuples(inBufferPages[i].get(0), nextTupleToAdd) < 0) {
                    System.out.println("page " + i+1 + " is null");
                    nextTupleToAdd = inBufferPages[i].get(0);
                    page = i;
                } else {
                    System.out.println("Both pages " + i + " and " + (i+1) + " are not null");
                    System.out.println("Size of page " + i + ": " + inBufferPages[i].size());
                    System.out.println("Size of page " + (i+1) + ": " + inBufferPages[i+1].size());
                    for (int j = 0; j < inBufferPages[i].size(); j++) {
                        System.out.print("Content in page " + i + ": " + inBufferPages[i].get(j).dataAt(0) + " ");
                    }
                    System.out.println();
                    for (int j = 0; j < inBufferPages[i+1].size(); j++) {
                        System.out.print("Content in page " + (i+1) + ": " + inBufferPages[i+1].get(j).dataAt(0) + " ");
                    }
                    System.out.println();
                    if (compareTuples(inBufferPages[i].get(0), nextTupleToAdd) < 0) {
                        nextTupleToAdd = inBufferPages[i].get(0);
                        page = i;
                    } else if (compareTuples(inBufferPages[i+1].get(0), nextTupleToAdd) < 0) {
                        nextTupleToAdd = inBufferPages[i+1].get(0);
                        page = i+1;
                    }
                }
            }
            if (nextTupleToAdd == null) {
                // no more tuples
                eos = true;
                System.out.println("No more tuples");
                break;
            }
            System.out.println("Removed from page: " + page);
            inBufferPages[page].remove(0);
            if (inBufferPages[page].isEmpty()) {
                Batch nextBatch = sortedRun.next();
                inBufferPages[page] = nextBatch;
                if (nextBatch == null) {
                    System.out.println("Next batch is null for page " + page);
                    break;
                }
                System.out.println("Getting next batch for page " + page + " with size " + nextBatch.size());
            }
            if (compareTuples(prevAddedTuple, nextTupleToAdd) == 0) {
                // duplicates
                System.out.println("Duplicate: " + nextTupleToAdd.dataAt(0) + " from page " + page);
                continue;
            }
            // Add to outbatch
            outbatch.add(nextTupleToAdd);
            System.out.println("Added: " + nextTupleToAdd.dataAt(0) + " taken from page " + page);
            prevAddedTuple = nextTupleToAdd;
        }
        System.out.println("size of outbatch: " + outbatch.size());
        return outbatch;
    }

    private int compareTuples(Tuple t1, Tuple t2) {
        if (t1 == null) {
            return 1;
        } else if (t2 == null) {
            return -1;
        }
        return Tuple.compareTuples(t1, t2, attrIndex, attrIndex);
    }

    public void setNumBuff(int numbuff) {
        this.numBuff = numbuff;
    }
}
