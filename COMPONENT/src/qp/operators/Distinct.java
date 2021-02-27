/**
 * To remove duplicates in the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

public class Distinct extends Operator {

    Operator base;                      // Base operator
    ArrayList<Attribute> attrset;       // Set of atttributes to project
    ArrayList<Integer> attrIndex;       // Index of attributes to project
    int batchsize;                      // Number of tuples per out batch
    int numbuff;                        // Number of buffers available
    int totalnumrun;                    // Number of sorted runs
    int totalnumpass;                   // Number of passes
    ObjectInputStream insorted;         // Input stream for sorted results
    boolean eos;                        // Whether end of stream is reached
    Tuple prevtuple;                    // Previous tuple

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

    public void setNumBuff(int num) {
        this.numbuff = num;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     * * and sort the table on the attributes to project removing duplicates
     **/
    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        Schema baseSchema = base.getSchema();
        attrIndex = new ArrayList<>(attrset.size());
        for (int i = 0; i < attrset.size(); i++) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggregation is not implemented.");
                System.exit(1);
            }

            attrIndex.add(baseSchema.indexOf(attr.getBaseAttribute()));
        }
        prevtuple = null;

        totalnumrun = 0;
        generateSortedRuns();
        mergeSortedRuns(1, totalnumrun);

        if (!base.close()) return false;

        return true;
    }

    /**
     * Read next tuple from sorted relation 
     * * and project relevant attributes removing duplicates
     */
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
                System.err.println("Distinct: Error in reading temporary file");
            }
            return outbatch;
        } catch (ClassNotFoundException c) {
            System.err.println("Distinct: Error in deserialising temporary file ");
            System.exit(1);
        } catch (IOException io) {
            System.err.println("Distinct: Error in reading temporary file");
            System.exit(1);
        }
        return outbatch;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Distinct newDistinct = new Distinct(newbase, newattr);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newDistinct.setSchema(newSchema);
        return newDistinct;
    }

    // compares tuples t1 and t2 based on the attributes to project
    private int compareTuples(Tuple t1, Tuple t2) {
        for (int index : attrIndex) {
            int res = Tuple.compareTuples(t1, t2, index, index);
            if (res != 0) {
                return res;
            }
        }
        return 0;
    }

    // generate sorted runs with duplicates removed from the base operator and write each run to a file
    private void generateSortedRuns() {
        Batch batch = base.next();
        while (batch != null) {
            // Ensure no duplicates when generating sorted runs as well
            TreeSet<Tuple> tuples = new TreeSet<>(this::compareTuples);
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
                    Tuple tuple = tuples.pollFirst();
                    outbatch.add(tuple);
                    if (outbatch.isFull()) {
                        out.writeObject(outbatch);
                        outbatch = new Batch(batchsize);
                    }
                }
                out.writeObject(outbatch);
                out.close();
            } catch (IOException e) {
                System.err.println("Distinct: error writing to temporary file");
                System.exit(1);
            }
            totalnumrun++;
        }
    }

    // merge sorted runs iteratively until one sorted run is produced
    private int mergeSortedRuns(int numpass, int numrun) {
        if (numrun == 1) {
            totalnumpass = numpass;
            try {
                String fname = "SortedRun-" + (numpass - 1) + "-" + (numrun - 1) + "-" + this.hashCode();
                insorted = new ObjectInputStream(new FileInputStream(fname));
            } catch (IOException e) {
                System.err.println("Distinct: error in reading temporary file");
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

    // merge sorted runs between start (inclusive) and end (exclusive) and remove duplicates
    // to produce one sorted run and write to file
    // note: end - start <= numbuff
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
            System.err.println("Distinct: error writing to temporary file");
            System.exit(1);
        }

        // read sorted run into buffers
        for (int i = 0; i < end - start; i++) {
            String fname = "SortedRun-" + (numpass - 1) + "-" + (start + i) + "-" + this.hashCode();
            try {
                instream[i] = new ObjectInputStream(new FileInputStream(fname));
            } catch (IOException io) {
                System.err.println("Distinct: error in reading temporary file");
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
                    System.err.println("Distinct: Error in reading temporary file");
                }
            } catch (ClassNotFoundException c) {
                System.err.println("Distinct: Error in deserialising temporary file ");
                System.exit(1);
            } catch (IOException io) {
                System.err.println("Distinct: Error in reading temporary file");
                System.exit(1);
            }
            if (batch != null && !batch.isEmpty()) {
                for (Tuple t : batch.getAllTuples()) {
                    pq.add(new TupleWithId(t, i, curs[i]));
                    curs[i]++;
                }
            } else {
                try {
                    instream[i].close();
                    String dfname = "SortedRun-" + (numpass - 1) + "-" + (start + i) + "-" + this.hashCode();
                    File f = new File(dfname);
                    f.delete();
                } catch (IOException io) {
                    System.err.println("Distinct: Error in reading temporary file");
                }
            }
        }

        // add next tuple to outbatch
        Batch outbatch = new Batch(batchsize);
        while (!pq.isEmpty()) {
            TupleWithId tuple = pq.poll();

            if (prevtuple == null || Tuple.compareTuples(prevtuple, tuple.tuple, attrIndex, attrIndex) != 0) { // not duplicate
                prevtuple = tuple.tuple;
                outbatch.add(tuple.tuple);
                if (outbatch.isFull()) {
                    try {
                        out.writeObject(outbatch);
                    } catch (IOException io) {
                        System.err.println("Distinct: error writing to temporary file");
                        System.exit(1);
                    }
                    outbatch = new Batch(batchsize);
                }
            }

            int runid = tuple.runid;
            int nexttupleid = tuple.tupleid + 1;

            // read next page for run into buffer if there are no more tuples for that sorted run
            if (!done[runid] && nexttupleid % batchsize == 0) {
                Batch batch = new Batch(batchsize);
                try {
                    batch = (Batch) instream[runid].readObject();
                } catch (EOFException e) {
                    try {
                        instream[runid].close();
                        done[runid] = true;
                        String dfname = "SortedRun-" + (numpass - 1) + "-" + (start + runid) + "-" + this.hashCode();
                        File f = new File(dfname);
                        f.delete();
                    } catch (IOException io) {
                        System.err.println("Distinct: Error in reading temporary file");
                    }
                } catch (ClassNotFoundException c) {
                    System.err.println("Distinct: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.err.println("Distinct: Error in reading temporary file");
                    System.exit(1);
                }
                if (batch != null && !batch.isEmpty()) {
                    for (Tuple t : batch.getAllTuples()) {
                        pq.add(new TupleWithId(t, runid, curs[runid]));
                        curs[runid]++;
                    }
                } else {
                    try {
                        instream[runid].close();
                        done[runid] = true;
                        String dfname = "SortedRun-" + (numpass - 1) + "-" + (start + runid) + "-" + this.hashCode();
                        File f = new File(dfname);
                        f.delete();
                    } catch (IOException io) {
                        System.err.println("Distinct: Error in reading temporary file");
                    }
                }
            }
        }

        if (!outbatch.isEmpty()) {
            try {
                out.writeObject(outbatch);
            } catch (IOException io) {
                System.err.println("Distinct: error writing to temporary file");
                System.exit(1);
            }
        }

        try {
            out.close();
        } catch (IOException io) {
            System.out.println("Distinct: Error in reading temporary file");
        }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        return true;
    }
}

