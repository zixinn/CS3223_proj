package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.*;

public class Sort extends Operator {
        
    Operator base;                  // Base Operator
    String fname;                   // The file name where the sorted run is materialized
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> attrIndex;   // Indexes of atttributes to sort
    int numBuff;                    // Number of buffers available
    Batch outbatch;                 // Buffer for output stream
    ArrayList<Tuple> tuples;        // The tuples in-memory
    ObjectInputStream in;           // File pointer to the sorted runs file
    boolean isAscending;            // Sort ascending or descending
    boolean eos_unsorted;           // Whether end of stream (unsorted result) is reached

    int numRun;                     // Count number of sorted runs generated
    ArrayList<String> tempFiles;    // Pages that are stored on the disk
    ArrayList<TupleReader> readers; // Store readers for merging
    ArrayList<Pair> pairs;          // Store pairs of tuples and index to its reader

    public Sort(Operator base, ArrayList<Integer> attrIndex, int numBuff, boolean isAscending) {
        super(OpType.SORT);
        this.base = base;
        this.schema = base.schema;
        this.numBuff = numBuff;
        this.attrIndex = attrIndex;
        this.isAscending = isAscending;
        this.tempFiles = new ArrayList<>();
        this.readers = new ArrayList<>();        
        
    }

    private void generateSortedRuns() {
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
                break;
            }

            // In-mem sorting          
            Collections.sort(tuples, (t1, t2) -> isAscending? Tuple.compareTuples(t1, t2, attrIndex, attrIndex) : Tuple.compareTuples(t1, t2, attrIndex, attrIndex) * -1);

            numRun++;
            fname = "SortedRun-" + String.valueOf(numRun);

            TupleWriter writer = new TupleWriter(fname, batchsize);
            if (!writer.open()) {
                System.out.println("SortedRun: Error writing sorted runs to file " + fname);
                System.exit(1);
            }
            while (!tuples.isEmpty()) {
                writer.next(tuples.remove(0));
            }
            writer.close();
     
            
            tempFiles.add(fname);
        }

        base.close();  
        
    }
    
    private void merge() {
        ArrayList<String> temp_next_sortedrun = new ArrayList<>();
        int i = 0;
        while (i < tempFiles.size()) {
            while (i < tempFiles.size() && i % (numBuff - 1) != 0) {
                //Add pages into buffer until input buffer full
                TupleReader reader = new TupleReader(tempFiles.get(i), batchsize);
                if (!reader.open()) {
                    System.out.println(tempFiles.get(i) + ": Unable to open file");
                    System.exit(1);
                }
                readers.add(reader);
                i++;
            }
            numRun++;
            fname = "SortedRun-" + String.valueOf(numRun);
            
            pairs.clear();
            //Populate tuples with first tuple from each buffer
            for (int j = 0; j < readers.size(); j++) {
                Tuple tuple = readers.get(j).next();
                if (tuple == null) {
                    System.out.println("tuple is empty");
                    System.exit(1);
                }
                pairs.add(new Pair(tuple, j));
            }

            Collections.sort(pairs, (p1, p2) -> isAscending? Tuple.compareTuples(p1.tuple, p2.tuple, attrIndex, attrIndex) : Tuple.compareTuples(p1.tuple, p2.tuple, attrIndex, attrIndex) * -1);

     
            TupleWriter writer = new TupleWriter(fname, batchsize);
            if (!writer.open()) {
                System.out.println("SortedRun: Error writing sorted runs to file " + fname);
                System.exit(1);
            }
            //Merge and place each element into the output buffer
            while (!pairs.isEmpty()) {
                Pair pair = pairs.remove(0);
                writer.next(pair.tuple);
                Tuple nextTuple = readers.get(pair.index).next();
                if (nextTuple != null) {
                    pairs.add(new Pair(nextTuple, pair.index));
                    Collections.sort(pairs, (p1, p2) -> isAscending? Tuple.compareTuples(p1.tuple, p2.tuple, attrIndex, attrIndex) : Tuple.compareTuples(p1.tuple, p2.tuple, attrIndex, attrIndex) * -1);
                }
            }
            writer.close();


            temp_next_sortedrun.add(fname);
            readers.clear();
        }
        
        //delete the temp files
        for (int k = 0; k < tempFiles.size(); k++) {
            File f = new File(tempFiles.get(k));
            f.delete();
        }

        tempFiles = temp_next_sortedrun;

    }
    
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;
        
        eos_unsorted = false;
        tuples = new ArrayList<>();
        pairs = new ArrayList<>();
        generateSortedRuns();

        while (tempFiles.size() > numBuff - 1) {
            merge();
        }

        //Load pages in disk into buffer
        for (int i = 0; i < tempFiles.size(); i++) {
            fname = tempFiles.get(i);
            TupleReader reader = new TupleReader(fname, batchsize);
            if (!reader.open()) {
                System.out.println("Unable to open file for: " + fname);
                System.exit(1);
                return false;
            }
            readers.add(reader);
        }

        pairs.clear();
        //Populate tuples with first tuple from each buffer
        for (int j = 0; j < readers.size(); j++) {
            Tuple tuple = readers.get(j).next();
            if (tuple == null) {
                System.out.println("tuple is empty");
                System.exit(1);
                return false;
            }
            pairs.add(new Pair(tuple, j));
        }

        Collections.sort(pairs, (p1, p2) -> isAscending? Tuple.compareTuples(p1.tuple, p2.tuple, attrIndex, attrIndex) : Tuple.compareTuples(p1.tuple, p2.tuple, attrIndex, attrIndex) * -1);


        return true;
    }

    public Batch next() {
        outbatch = new Batch(batchsize);
        while (!pairs.isEmpty()) {
            Pair pair = pairs.remove(0);
            outbatch.add(pair.tuple);
            Tuple nextTuple = readers.get(pair.index).next();
            if (nextTuple != null) {
                pairs.add(new Pair(nextTuple, pair.index));
                Collections.sort(pairs, (p1, p2) -> isAscending? Tuple.compareTuples(p1.tuple, p2.tuple, attrIndex, attrIndex) : Tuple.compareTuples(p1.tuple, p2.tuple, attrIndex, attrIndex) * -1);

            }
            if (outbatch.isFull()) {
                return outbatch;
            }
        }
        if (outbatch.isEmpty()) {
            return null;
        }
        return outbatch;
    }

    public boolean close() {
        for (int i = 0; i < readers.size(); i++) {
            readers.get(i).close();
        }
        //delete the temp files
        for (int k = 0; k < tempFiles.size(); k++) {
            File f = new File(tempFiles.get(k));
            f.delete();
        }
        return true;
    }

    class Pair {
        Tuple tuple;
        int index;

        Pair(Tuple t, int i) {
            this.tuple = t;
            this.index = i;
        }
    }


}