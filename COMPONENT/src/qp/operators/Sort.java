/**
 * Sort algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

public class Sort extends Operator {

    Operator base;                  // Base Operator
    int batchsize;                  // Number of tuples per out batch
    int numbuff;                    // Number of buffers available
    int totalnumrun;                // Number of sorted runs
    int totalnumpass;               // Number of passes

    ArrayList<Attribute> attrset;   // Set of attributes to project
    ArrayList<Integer> attrIndex;   // Indexes of atttributes to sort
    ObjectInputStream insorted;     // Input stream for sorted results

    boolean eos = false;            // Whether the end of stream is reached
    boolean isAscending;            // Whether to sort ascending or descending

    public Sort(Operator base, ArrayList<Attribute> as, int numbuff, boolean isAscending) {
        super(OpType.SORT);
        this.base = base;
        this.schema = base.schema;
        this.numbuff = numbuff;
        this.attrset = as;
        this.isAscending = isAscending;
    }


    private int compareTuples(Tuple t1, Tuple t2) {
        for (int index : attrIndex) {
            int res = Tuple.compareTuples(t1, t2, index, index);
            if (res != 0) {
                return isAscending? res : res * -1;
            }
        }
        return 0;
    }

    private void generateSortedRuns() {
        Batch batch = base.next();
        while (batch != null) {
            PriorityQueue<Tuple> tuples = new PriorityQueue<>(batchsize, this::compareTuples);
            for (int i = 0; i < numbuff && batch != null; i++) {
                for (int j = 0; j < batch.size(); j++) {
                    tuples.add(batch.get(j));
                }
                batch = base.next();
            }

            String fname = "SortedRun-" + 0 + "-" + totalnumrun + "-" + this.hashCode();
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fname));
                Batch outbatch = new Batch(batchsize);
                while (!tuples.isEmpty()) {
                    Tuple tuple = tuples.poll();
                    outbatch.add(tuple);
                    if (outbatch.isFull()) {
                        out.writeObject(outbatch);
                        outbatch = new Batch(batchsize);
                    }
                }
                out.writeObject(outbatch);
                out.close();
            } catch (IOException e) {
                System.err.println("Sort: error writing to temporary file");
                System.exit(1);
            }
            totalnumrun++;
        }
    }

    private int mergeSortedRuns(int numpass, int numrun) {
        if (numrun == 1) {
            totalnumpass = numpass;
            try {
                String fname = "SortedRun-" + (numpass - 1) + "-" + (numrun - 1) + "-" + this.hashCode();
                insorted = new ObjectInputStream(new FileInputStream(fname));
            } catch (IOException e) {
                System.err.println("Sort: error in reading temporary file");
            }
            return numrun;
        }

        int numoutrun = 0;
        int start = 0;
        while (start < numrun) {
            int end = start + numbuff - 1 < numrun ? start + numbuff - 1 : numrun;
            mergeSortedRunsRange(start, end, numpass, numoutrun);
            numoutrun++;
            start += numbuff - 1;
        }
        return mergeSortedRuns(numpass + 1, numoutrun);
    }

    private void mergeSortedRunsRange(int start, int end, int numpass, int numrun) {
        ObjectInputStream[] instream = new ObjectInputStream[end - start];
        PriorityQueue<TupleWithId> pq = new PriorityQueue<>(batchsize, (t1, t2) -> compareTuples(t1.tuple, t2.tuple));
        int[] curs = new int[end - start];
        boolean[] done = new boolean[end - start];

        String ofname = "SortedRun-" + numpass + "-" + numrun + "-" + this.hashCode();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(new FileOutputStream(ofname));
        } catch (IOException io) {
            System.err.println("Sort: error writing to temporary file");
            System.exit(1);
        }

        // read sorted run into buffers
        for (int i = 0; i < end - start; i++) {
            String fname = "SortedRun-" + (numpass - 1) + "-" + (start + i) + "-" + this.hashCode();
            try {
                instream[i] = new ObjectInputStream(new FileInputStream(fname));
            } catch (IOException io) {
                System.err.println("Sort: error in reading temporary file");
                System.exit(1);
            }
            
            Batch batch = new Batch(batchsize);
            try {
                batch = (Batch) instream[i].readObject();
            } catch (EOFException e) {
                try {
                    instream[i].close();
                    String dfname = "SortedRun-" + (numpass - 1) + "-" + (start + i) + "-" + this.hashCode();
                    File f = new File(dfname);
                    f.delete();
                } catch (IOException io) {
                    System.err.println("Sort: Error in reading temporary file");
                }
            } catch (ClassNotFoundException c) {
                System.err.println("Sort: Error in deserialising temporary file ");
                System.exit(1);
            } catch (IOException io) {
                System.err.println("Sort: Error in reading temporary file");
                System.exit(1);
            }
            if (batch != null && !batch.isEmpty()) {
                for (Tuple t : batch.getAllTuples()) {
                    pq.add(new TupleWithId(t, i, curs[i]));
                    curs[i]++;
                }
            }
        }

        Batch outbatch = new Batch(batchsize);
        while (!pq.isEmpty()) {
            TupleWithId tuple = pq.poll();
            outbatch.add(tuple.tuple);
            if (outbatch.isFull()) {
                try {
                    out.writeObject(outbatch);
                } catch (IOException io) {
                    System.err.println("Sort: error writing to temporary file");
                    System.exit(1);
                }
                outbatch = new Batch(batchsize);
            }

            int runid = tuple.runid;
            int nexttupleid = tuple.tupleid + 1;

            // read next page for run into buffer
            if (!done[runid] && nexttupleid % batchsize == 0) {
                Batch batch = new Batch(batchsize);
                try {
                    batch = (Batch) instream[runid].readObject();
                } catch (EOFException e) {
                    try {
                        instream[runid].close();
                    } catch (IOException io) {
                        System.err.println("Sort: Error in reading temporary file");
                    }
                    done[runid] = true;
                    String dfname = "SortedRun-" + (numpass - 1) + "-" + (start + runid) + "-" + this.hashCode();
                    File f = new File(dfname);
                    f.delete();
                } catch (ClassNotFoundException c) {
                    System.err.println("Sort: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.err.println("Sort: Error in reading temporary file");
                    System.exit(1);
                }
                if (batch != null && !batch.isEmpty()) {
                    for (Tuple t : batch.getAllTuples()) {
                        pq.add(new TupleWithId(t, runid, curs[runid]));
                        curs[runid]++;
                    }
                }
            }
        }

        if (!outbatch.isEmpty()) {
            try {
                out.writeObject(outbatch);
            } catch (IOException io) {
                System.err.println("Sort: error writing to temporary file");
                System.exit(1);
            }
        }

        try {
            out.close();
        } catch (IOException io) {
            System.out.println("NestedJoin: Error in reading temporary file");
        }
    }

    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;
        
        Schema baseSchema = base.getSchema();
        attrIndex = new ArrayList<>(attrset.size());
        for (int i = 0; i < attrset.size(); i++) {
            Attribute attr = attrset.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex.add(schema.indexOf(attr));
        }

        totalnumrun = 0;
        generateSortedRuns();
        mergeSortedRuns(1, totalnumrun);

        if (!base.close()) return false;

        return true;
    }

    public Batch next() {
        if (eos) {
            close();
            return null;
        }

        Batch outbatch = new Batch(batchsize);
        try {
            outbatch = (Batch) insorted.readObject();
        } catch (EOFException e) {
            eos = true;
            try {
                insorted.close();
                String dfname = "SortedRun-" + (totalnumpass - 1) + "-" + 0 + "-" + this.hashCode();
                File f = new File(dfname);
                f.delete();
            } catch (IOException io) {
                System.err.println("Sort: Error in reading temporary file");
            }
            return outbatch;
        } catch (ClassNotFoundException c) {
            System.err.println("Sort: Error in deserialising temporary file ");
            System.exit(1);
        } catch (IOException io) {
            System.err.println("Sort: Error in reading temporary file");
            System.exit(1);
        }
        return outbatch;
    }

    public boolean close() {
        return true;
    }
}

class TupleWithId {
    Tuple tuple;
    int runid;
    int tupleid;

    public TupleWithId(Tuple tuple, int runid, int tupleid) {
        this.tuple = tuple;
        this.runid = runid;
        this.tupleid = tupleid;
    }
}
