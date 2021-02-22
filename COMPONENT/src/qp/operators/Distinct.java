package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

public class Distinct extends Operator {

    Operator base;                  // Base Operator
    String fname;                   // The file name where the sorted run is materialized
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> attrIndex;   // Indexes of atttributes to sort
    ArrayList<Attribute> attrset;   // Set of atttributes to sort
    int numBuff;                    // Number of buffers available
    Batch outbatch;                 // Buffer for output stream
    ArrayList<Tuple> tuples;        // The tuples in-memory
    ObjectInputStream in;           // File pointer to the sorted runs file
    ArrayList<ObjectInputStream> sortedRunFiles; // Input file pointers for each sorted run

    int numRun;                     // Count number of sorted runs generated
    int runcurs;                    // Cursor to keep track of which file we are at
    boolean eos_unsorted;           // Whether end of stream (unsorted result) is reached
    boolean eos_sorted;             // Whether end of stream (sorted result) is reached

    public Distinct(Operator base, ArrayList<Attribute> attrset) {
        super(OpType.DISTINCT);
        this.base = base;
        this.schema = base.schema;
        this.attrset = attrset;
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

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        eos_unsorted = false;
        runcurs = 0;
        eos_sorted = false;
        tuples = new ArrayList<>();
        sortedRunFiles = new ArrayList<>();

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

        boolean success;
        success = generateSortedRuns();

        // open new input streams to read from sortedRuns
        for (int i = 1; i <= numRun; i++) {
            fname = "SortedRun-" + String.valueOf(i);
            try {
                sortedRunFiles.add(new ObjectInputStream(new FileInputStream(fname)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        success = success && multiWayMerge();
        try {
            fname = "Merge-" + String.valueOf(runcurs);
            System.out.println(" ~~~~~~ READING FROM: " + fname);
            in = new ObjectInputStream(new FileInputStream(fname));
        } catch (IOException e) {
            System.err.println("Distinct:error in reading the merged run file");
            System.exit(1);
        }
        return success;
    }

    public Batch next() {
        outbatch = new Batch(batchsize);
        if (eos_sorted) {
            close();
            return null;
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
            System.err.println("Distinct:error in reading the sorted run file");
            System.exit(1);
        } catch (ClassNotFoundException c) {
            System.out.println("Distinct: Error in deserialising sorted run file ");
            System.exit(1);
        }
        for (int i = 0; i < outbatch.size(); i++) {
            System.out.print("Content read: " + outbatch.get(i).dataAt(0) + " ");
        }
        System.out.println();

        return outbatch;
    }

    private boolean multiWayMerge() {
        int start = 0;
        int stop;
        while (numRun - 1 > start) {
            if (numRun - start > numBuff - 1) {
                stop = start + numBuff - 2;
            } else {
                stop = numRun - 1;
            }
            merge(start, stop);
            start = stop;
        }
        return true;
    }

    private boolean merge(int start, int stop) {
        //sortedRunFiles.clear();
        runcurs++;

        System.out.println("========== Run curs: " + runcurs + " Numrun " + numRun + " Start " + start + " stop: " + stop + " numbuff - 1: " + (numBuff-1));
        boolean[] eof = new boolean[stop - start + 1];   // keep track of eof of each file
        Batch[] inBufferPages = new Batch[stop - start + 1];;          // Buffer pages for merging

        ObjectInputStream merged = null;

        // Load the sorted runs into the input buffers
        if (start == 0) {
            // first pass --> load all batches from SortedRun files
            Batch nextBatch = null;
            try {
                nextBatch = (Batch) sortedRunFiles.get(0).readObject();
            } catch (EOFException e) {
                eof[0] = true;
                nextBatch = null;
            } catch (IOException io) {
                System.err.println("Distinct:error in reading the sorted run file");
                System.exit(1);
            } catch (ClassNotFoundException c) {
                System.out.println("Distinct: Error in deserialising sorted run file ");
                System.exit(1);
            }
            inBufferPages[0] = nextBatch;
        } else {
            // not first pass --> load a batch from the Merged file, the rest from sortedRun files
            Batch nextBatch = null;
            try {
                merged = new ObjectInputStream(new FileInputStream("Merge-" + (runcurs-1)));
                nextBatch = (Batch) merged.readObject();
            } catch (EOFException e) {
                eof[0] = true;
                nextBatch = null;
            } catch (IOException io) {
                System.err.println("Distinct:error in reading the sorted run file");
                System.exit(1);
            } catch (ClassNotFoundException c) {
                System.out.println("Distinct: Error in deserialising sorted run file ");
                System.exit(1);
            }
            inBufferPages[0] = nextBatch;
        }

        for (int i = start + 1; i < stop; i++) {
            Batch nextBatch = null;
            try {
                nextBatch = (Batch) sortedRunFiles.get(i).readObject();
            } catch (EOFException e) {
                eof[i-start] = true;
                nextBatch = null;
            } catch (IOException io) {
                System.err.println("Distinct:error in reading the sorted run file");
                System.exit(1);
            } catch (ClassNotFoundException c) {
                System.out.println("Distinct: Error in deserialising sorted run file ");
                System.exit(1);
            }
            inBufferPages[i-start] = nextBatch;
        }

        boolean eos = true; // eos for everything
        Batch outbatch = new Batch(batchsize);
        Tuple prevAddedTuple = null;
        Tuple nextTupleToAdd;
        int page;
        for (int i = 0; i < stop - start + 1; i++) {
            if (!eof[i]) {
                eos = false;
                break;
            }
        }
        if (eos) {
            return true;
        }

        // materialise file to write output for merge
        ObjectOutputStream out;
        fname = "Merge-" + String.valueOf(runcurs);
        try {
            out = new ObjectOutputStream(new FileOutputStream(fname));
        } catch (IOException io) {
            System.out.println("Distinct: Error writing merged stuff to file " + fname);
            return false;
        }

        while (!eos) {
            outbatch = new Batch(batchsize);
            while (!outbatch.isFull()) {
                nextTupleToAdd = null;
                page = -1; // page that we took the nextTupleToAdd from
                // Find the next tuple to add
                for (int i = 0; i < stop - start; i++) {
                    //System.out.println("i: " + i);
                    if (inBufferPages[i] == null && inBufferPages[i + 1] == null) {
                        System.out.println("Both buffer pages are null. i is " + i + " Num of loops in for loop: " + (stop-start));
                        continue;
                    } else if (inBufferPages[i] == null) {
                        System.out.println("page " + i + " is null");
                        if (compareTuples(inBufferPages[i + 1].get(0), nextTupleToAdd) <= 0) {
                            nextTupleToAdd = inBufferPages[i + 1].get(0);
                            page = i + 1;
                        }
                    } else if (inBufferPages[i + 1] == null) {
                        System.out.println("page " + (i + 1) + " is null");
                        if (compareTuples(inBufferPages[i].get(0), nextTupleToAdd) <= 0) {
                            nextTupleToAdd = inBufferPages[i].get(0);
                            page = i;
                        }
                    } else {
                        System.out.println("Both pages " + i + " and " + (i + 1) + " are not null");
                        for (int j = 0; j < inBufferPages[i].size(); j++) {
                            System.out.print("Content in page " + i + ": " + inBufferPages[i].get(j).dataAt(0) + " ");
                        }
                        System.out.println();
                        for (int j = 0; j < inBufferPages[i + 1].size(); j++) {
                            System.out.print("Content in page " + (i + 1) + ": " + inBufferPages[i + 1].get(j).dataAt(0) + " ");
                        }
                        System.out.println();
                        if (compareTuples(inBufferPages[i].get(0), inBufferPages[i+1].get(0)) <= 0) {
                            if (compareTuples(inBufferPages[i].get(0), nextTupleToAdd) <= 0) {
                                nextTupleToAdd = inBufferPages[i].get(0);
                                page = i;
                            }
                        } else if (compareTuples(inBufferPages[i + 1].get(0), nextTupleToAdd) <= 0) {
                            nextTupleToAdd = inBufferPages[i + 1].get(0);
                            page = i + 1;
                        }
                    }
                }
                if (nextTupleToAdd == null) {
                    // no more tuples
                    eos = true;
                    System.out.println("No more tuples");
                    break;
                }
                inBufferPages[page].remove(0);
                System.out.println("Removed from page: " + page + ". Size is now: " + inBufferPages[page].size() + " Is empty: " + inBufferPages[page].isEmpty());
                if (inBufferPages[page].isEmpty()) {
                    System.out.println("empty");
                    try {
                        Batch nextBatch;
                        if (start == 0 || page > 0) {
                            nextBatch = (Batch) sortedRunFiles.get(page+start).readObject();
                        } else {
                            nextBatch = (Batch) merged.readObject();
                        }
                        inBufferPages[page] = nextBatch;
                        if (nextBatch == null || nextBatch.isEmpty()) {
                            System.out.println("Next batch is null for page " + page);
                            eof[page] = true;
                            inBufferPages[page] = null;
                        }
                        System.out.println("Getting next batch for page " + page + " with size " + nextBatch.size());
                    } catch (EOFException e) {
                        System.out.println("EOF");
                        eof[page] = true;
                        inBufferPages[page] = null;
                    } catch (IOException io) {
                        io.printStackTrace();
                        System.err.println("Distinct :error in reading the sorted run file");
                        System.exit(1);
                    } catch (ClassNotFoundException c) {
                        System.out.println("Distinct: Error in deserialising sorted run file ");
                        System.exit(1);
                    }
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

            // write to sorted run last file
            try {
                System.out.println("            size of outbatch: " + outbatch.size());
                for (int i = 0; i < outbatch.size(); i++) {
                    System.out.println("Content in outbatch: " + outbatch.get(i).dataAt(0));
                }
                out.writeObject(outbatch);
            } catch (IOException io) {
                System.out.println("Distinct: Error writing sorted runs to file");
                return false;
            }
            // TODO: have boolean for last merge and dont write to file
        }
        try {
            System.out.println("Closing ObjectOutputStream with fname: " + fname);
            out.close();

            // close all input streams
//            for (int i = 0; i < sortedRunFiles.size(); i++) {
//                sortedRunFiles.get(i).close();
//            }

        } catch (IOException io) {
            System.out.println("Distinct: Error writing sorted runs to file");
            return false;
        }

        return true;
    }

    private int compareTuples(Tuple t1, Tuple t2) {
        if (t1 == null) {
            return 1;
        } else if (t2 == null) {
            return -1;
        }
        return Tuple.compareTuples(t1, t2, attrIndex, attrIndex);
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
                System.out.println("Distinct: Error writing sorted runs to file");
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
