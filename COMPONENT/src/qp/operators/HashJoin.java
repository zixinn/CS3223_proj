/**
 * Hash Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.RandNumb;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class HashJoin extends Join{

    int leftbatchsize;              // Number of tuples per left batch
    int rightbatchsize;             // Number of tuples per right batch
    int batchsize;                  // Number of tuples per out batch
    int leftnumpages;               // Number of pages for left table
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String lfname;                  // The file name where the left table is materialized
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream inleft;       // File pointer to the left hand materialized file
    ObjectInputStream inright;      // File pointer to the right hand materialized file
    ArrayList<Batch> hashtable;     // Hash table for joining phase
    Join hashjoin;                  // Hash join for recursive hash join
    Join blocknestedjoin;           // Block nested join for partitions that cannot fit into memory

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    int pcurs;                      // Cursor for partition
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached
    boolean done;                   // Whether the hash join is completed

    int a1, b1, a2, b2;             // Constants for hash functions

    public HashJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();

        a1 = RandNumb.randInt(0, numBuff);
        b1 = RandNumb.randInt(0, numBuff);
        a2 = RandNumb.randInt(0, numBuff);
        b2 = RandNumb.randInt(0, numBuff);
    }

    /**
     * During open finds the index of the join attributes
     * * Partitions the left and right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int lefttuplesize = left.schema.getTupleSize();
        leftbatchsize = Batch.getPageSize() / lefttuplesize;
        int righttuplesize = right.schema.getTupleSize();
        rightbatchsize = Batch.getPageSize() / righttuplesize;
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        leftnumpages = 0;
        hashjoin = null;
        blocknestedjoin = null;

        /** initialize the cursors of input buffers **/
        lcurs = 0; 
        rcurs = 0;
        pcurs = -1;
        eosl = true;
        eosr = true;
        done = false;

        /** Left hand side table is to be materialized
         ** for the left partition to perform
         **/
        if (!left.open()) {
            return false;
        } else {
            ArrayList<Batch> partitions = new ArrayList<>(numBuff - 1);
            for (int i = 0; i < numBuff - 1; i++) {
                partitions.add(new Batch(leftbatchsize));
            }
            try {
                ArrayList<ObjectOutputStream> leftout = new ArrayList<>(numBuff - 1);
                for (int i = 0; i < numBuff - 1; i++) {
                    String tfname =  "HJLtemp-" + String.valueOf(i) + this.hashCode();
                    leftout.add(new ObjectOutputStream(new FileOutputStream(tfname)));
                }
                while ((leftbatch = left.next()) != null) {
                    leftnumpages++;
                    for (int i = 0; i < leftbatch.size(); i++) {
                        Tuple tuple = leftbatch.get(i);
                        int key = tuple.dataAt(leftindex.get(0)).hashCode();
                        int partitionnum = (a1 * key + b1) % (numBuff - 1);
                        partitions.get(partitionnum).add(tuple);
                        if (partitions.get(partitionnum).isFull()) {
                            leftout.get(partitionnum).writeObject(partitions.get(partitionnum));
                            partitions.set(partitionnum, new Batch(leftbatchsize));
                        }
                    }
                }
                for (int i = 0; i < numBuff - 1; i++) {
                    if (!partitions.get(i).isEmpty()) {
                        leftout.get(i).writeObject(partitions.get(i));
                    }
                    leftout.get(i).close();
                }
            } catch (IOException io) {
                System.out.println("HashJoin: Error writing to temporary file");
                return false;
            }
            if (!left.close()) {
                return false;
            }
        }

        /** Right hand side table is to be materialized
         ** for the right partition to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            ArrayList<Batch> partitions = new ArrayList<>(numBuff - 1);
            for (int i = 0; i < numBuff - 1; i++) {
                partitions.add(new Batch(rightbatchsize));
            }
            try {
                ArrayList<ObjectOutputStream> rightout = new ArrayList<>(numBuff - 1);
                for (int i = 0; i < numBuff - 1; i++) {
                    String tfname =  "HJRtemp-" + String.valueOf(i) + this.hashCode();
                    rightout.add(new ObjectOutputStream(new FileOutputStream(tfname)));
                }
                while ((rightbatch = right.next()) != null) {
                    for (int i = 0; i < rightbatch.size(); i++) {
                        Tuple tuple = rightbatch.get(i);
                        int key = tuple.dataAt(rightindex.get(0)).hashCode();
                        int partitionnum = (a1 * key + b1) % (numBuff - 1);
                        partitions.get(partitionnum).add(tuple);
                        if (partitions.get(partitionnum).isFull()) {
                            rightout.get(partitionnum).writeObject(partitions.get(partitionnum));
                            partitions.set(partitionnum,new Batch(rightbatchsize));
                        }
                    }
                }
                for (int i = 0; i < numBuff - 1; i++) {
                    if (!partitions.get(i).isEmpty()) {
                        rightout.get(i).writeObject(partitions.get(i));
                    }
                    rightout.get(i).close();
                }
            } catch (IOException io) {
                System.out.println("HashJoin: Error writing to temporary file");
                return false;
            }
            return right.close();
        }
    }

    /**
     * from partitions selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        if (done) {
            return null;
        }

        // if (numBuff < Math.sqrt(leftnumpages)) {
        //     if (pcurs == -1) {
        //         pcurs++;
        //     }

        //     // recursively partition each partition
        //     while (pcurs < numBuff - 1) {
        //         if (hashjoin == null) {
        //             lfname = "HJLtemp-" + String.valueOf(pcurs) + this.hashCode();
        //             rfname = "HJRtemp-" + String.valueOf(pcurs) + this.hashCode();
        //             Scan s1 = new Scan(lfname, OpType.SCAN, true);
        //             s1.setSchema(left.schema);
        //             Scan s2 = new Scan(rfname, OpType.SCAN, true);
        //             s2.setSchema(right.schema);
        //             Join j = new Join(s1, s2, this.conditionList, this.optype);
        //             j.setSchema(schema);
        //             j.setJoinType(JoinType.HASHJOIN);
        //             j.setNumBuff(numBuff);
        //             hashjoin = new HashJoin(j);
        //             hashjoin.open();
        //         }

        //         outbatch = hashjoin.next();
        //         if (outbatch == null) {
        //             hashjoin.close();
        //             hashjoin = null;
        //             pcurs++;
        //         } else {
        //             return outbatch;
        //         }
        //     }

        //     return null;

        // } else {
            outbatch = new Batch(batchsize);
            while (!outbatch.isFull()) {
                if (lcurs == 0 && rcurs == 0 && eosr == true) {
                    if (blocknestedjoin != null) {
                        outbatch = blocknestedjoin.next();
                        if (outbatch == null) {
                            blocknestedjoin.close();
                            blocknestedjoin = null;
                            outbatch = new Batch(batchsize);
                        } else {
                            return outbatch;
                        }
                    }

                    if (pcurs == numBuff - 2 && eosl == true) {
                        done = true;
                        return outbatch;
                    }

                    if (eosl == true) {
                        pcurs++;
                        lfname = "HJLtemp-" + String.valueOf(pcurs) + this.hashCode();
                        rfname = "HJRtemp-" + String.valueOf(pcurs) + this.hashCode();
                    }

                    try {
                        inleft = new ObjectInputStream(new FileInputStream(lfname));
                        inright = new ObjectInputStream(new FileInputStream(rfname));
                    } catch (IOException e) {
                        System.err.println("HashJoin:error in reading the file");
                        System.exit(1);
                    }

                    hashtable = new ArrayList<>(numBuff - 2);
                    for (int i = 0; i < numBuff - 2; i++) {
                        hashtable.add(new Batch(leftbatchsize));
                    }

                    eosl = false;
                    eosr = false;
                    boolean full = false;

                    // read left partition
                    while (eosl == false && full == false) {
                        try {
                            leftbatch = (Batch) inleft.readObject();
                        } catch (EOFException e) {
                            try {
                                inleft.close();
                            } catch (IOException io) {
                                System.out.println("HashJoin: Error in reading temporary file");
                            }
                            eosl = true;
                            break;
                        } catch (ClassNotFoundException c) {
                            System.out.println("HashJoin: Error in deserialising temporary file ");
                            System.exit(1);
                        } catch (IOException io) {
                            System.out.println("HashJoin: Error in reading temporary file");
                            System.exit(1);
                        }

                        for (int i = 0; i < leftbatch.size(); i++) {
                            Tuple tuple = leftbatch.get(i);
                            int key = tuple.dataAt(leftindex.get(0)).hashCode();
                            int partitionnum = (a2 * key + b2) % (numBuff - 2);
                            hashtable.get(partitionnum).add(tuple);
                            if (hashtable.get(partitionnum).size() >= leftbatchsize) { // partition cannot fit into memory
                                lcurs = 0;
                                rcurs = 0;
                                eosl = true;
                                eosr = true;
                                full = true;
                                break;
                            }
                        }
                    }

                    if (full == true) {
                        // use block nested join if partition cannot fit in memory
                        Scan s1 = new Scan(lfname, OpType.SCAN, true);
                        s1.setSchema(left.schema);
                        Scan s2 = new Scan(rfname, OpType.SCAN, true);
                        s2.setSchema(right.schema);
                        Join j = new Join(s1, s2, this.conditionList, this.optype);
                        j.setSchema(schema);
                        j.setJoinType(JoinType.BLOCKNESTED);
                        j.setNumBuff(numBuff);
                        blocknestedjoin = new BlockNestedJoin(j);
                        blocknestedjoin.open();
                        outbatch = blocknestedjoin.next();
                        if (outbatch == null) {
                            blocknestedjoin.close();
                            blocknestedjoin = null;
                            outbatch = new Batch(batchsize);
                        } else {
                            return outbatch;
                        }
                    } else {
                        // read right partition
                        try {
                            rightbatch = (Batch) inright.readObject();
                        } catch (EOFException e) {
                            try {
                                inright.close();
                            } catch (IOException io) {
                                System.out.println("HashJoin: Error in reading temporary file");
                            }
                            eosr = true;
                            lcurs = 0;
                            rcurs = 0;
                            continue;
                        } catch (ClassNotFoundException c) {
                            System.out.println("HashJoin: Error in deserialising temporary file ");
                            System.exit(1);
                        } catch (IOException io) {
                            System.out.println("HashJoin: Error in reading temporary file");
                            System.exit(1);
                        }
                    }
                }

                while (eosr == false) {
                    for (int j = rcurs; j < rightbatch.size(); j++) {
                        Tuple righttuple = rightbatch.get(j);
                        int key = righttuple.dataAt(rightindex.get(0)).hashCode();
                        int partitionnum = (a2 * key + b2) % (numBuff - 2);
                        for (int i = lcurs; i < hashtable.get(partitionnum).size(); i++) {
                            Tuple lefttuple = hashtable.get(partitionnum).get(i);
                            if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                Tuple outtuple = lefttuple.joinWith(righttuple);
                                outbatch.add(outtuple);
                                if (outbatch.isFull()) {
                                    if (i == hashtable.get(partitionnum).size() - 1 && j != rightbatch.size() - 1) {
                                        lcurs = 0;
                                        rcurs = j + 1;
                                    } else {
                                        lcurs = i + 1;
                                        rcurs = j;
                                    }
                                    return outbatch;
                                }
                            }
                        }
                        lcurs = 0;
                    }
                    rcurs = 0;

                    try {
                        rightbatch = (Batch) inright.readObject();
                    } catch (EOFException e) {
                        try {
                            inright.close();
                        } catch (IOException io) {
                            System.out.println("HashJoin: Error in reading temporary file");
                        }
                        eosr = true;
                    } catch (ClassNotFoundException c) {
                        System.out.println("HashJoin: Error in deserialising temporary file ");
                        System.exit(1);
                    } catch (IOException io) {
                        System.out.println("HashJoin: Error in reading temporary file");
                        System.exit(1);
                    }
                }
            }

            return outbatch;
        // }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        for(int i = 0; i < numBuff - 1; i++) {
            String lfname = "HJLtemp-" + String.valueOf(i) + this.hashCode();
            File lf = new File(lfname);
            lf.delete();
            String rfname = "HJRtemp-" + String.valueOf(i) + this.hashCode();
            File rf = new File(rfname);
            rf.delete();
        }
        return true;
    }

}
