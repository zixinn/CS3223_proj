package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

public class SortedRun extends Operator {

    Operator base;                  // Base Operator
    String fname;                   // The file name where the sorted run is materialized
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> attrIndex;   // Indexes of atttributes to sort
    int numBuff;                    // Number of buffers available
    Batch outbatch;                 // Buffer for output stream
    ArrayList<Tuple> tuples;        // The tuples in-memory
    ObjectInputStream in;           // File pointer to the sorted runs file

    int numRun;                     // Count number of sorted runs generated
    int runcurs;                    // Cursor to keep track of which file we are at
    boolean eos_unsorted;           // Whether end of stream (unsorted result) is reached
    boolean eos_sorted;             // Whether end of stream (sorted result) is reached

    public SortedRun(Operator base, ArrayList<Integer> attrIndex, int numBuff) {
        super(OpType.SORT);
        this.base = base;
        this.schema = base.schema;
        this.numBuff = numBuff;
        this.attrIndex = attrIndex;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public int getNumRun() {
        return numRun;
    }

    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        eos_unsorted = false;
        runcurs = 0;
        eos_sorted = true;
        tuples = new ArrayList<>();

        if (!base.open()) return false;


        return generateSortedRuns();
    }

    public Batch next() {
        if (runcurs == numRun && eos_sorted) {
            close();
            return null;
        }

        if (eos_sorted) {
            // move on to next file
            runcurs++;
            eos_sorted = false;
            fname = "SortedRun-" + String.valueOf(runcurs);
            try {
                in = new ObjectInputStream(new FileInputStream(fname));
            } catch (IOException e) {
                System.err.println("SortedRun:error in reading the sorted run file");
                System.exit(1);
            }
        }

        try {
            outbatch = (Batch) in.readObject();
        } catch (EOFException e) {
            try {
                in.close();
                eos_sorted = true;
            } catch (IOException ioException) {
                System.exit(1);
            }
        } catch (IOException io) {
            System.err.println("SortedRun:error in reading the sorted run file");
            System.exit(1);
        } catch (ClassNotFoundException c) {
            System.out.println("SortedRun: Error in deserialising sorted run file ");
            System.exit(1);
        }

        return outbatch;
    }

    private boolean generateSortedRuns() {
        while (!eos_unsorted) {

            for (int i = 0; i < numBuff; i++) {
                Batch batch = base.next();
                if (batch == null) {
                    eos_unsorted = true;
                    break;
                }
                for (int j = 0; j < batch.size(); j++) {
                    tuples.add(batch.get(j));
                }
            }

            if (tuples.isEmpty()) {
                return true;
            }

            // In-mem sorting
            Collections.sort(tuples, (t1, t2) -> Tuple.compareTuples(t1, t2, attrIndex, attrIndex));

            numRun++;
            fname = "SortedRun-" + String.valueOf(numRun);
            // Write sorted runs to files
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fname));
                while (!tuples.isEmpty()) {
                    Batch batch = new Batch(batchsize);
                    while (!batch.isFull() && !tuples.isEmpty()) {
                        batch.add(tuples.remove(0));
                    }
                    out.writeObject(batch);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("SortedRun: Error writing sorted runs to file");
                return false;
            }

        }
        return true;
    }

    // delete all sorted run files
    public boolean close() {
        for (int i = 1; i <= numRun; i++) {
            File f = new File("SortedRun-" + String.valueOf(i));
            f.delete();
        }
        return true;
    }
}
