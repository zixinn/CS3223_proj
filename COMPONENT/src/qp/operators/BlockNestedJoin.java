/**
 * Block Nested Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    int blocksize;                  // No. of blocks for the outer (left) table
    ArrayList<Batch> leftblock;     // Buffer pages for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lblockcurs;                 // Cursor for left block
    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        blocksize = numBuff - 2;
        leftblock = new ArrayList<>();

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lblockcurs = 0;
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        int i, j;
        if (eosl) {
            return null;
        }
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (lcurs == 0 && lblockcurs == 0 && eosr == true) {
                /** new left block is to be fetched**/
                leftblock.clear();
                for (int k = 0; k < blocksize; k++) {
                    Batch leftbatch = (Batch) left.next();
                    if (leftbatch == null) {
                        eosl = true;
                        break;
                    }
                    leftblock.add(leftbatch);
                }
                if (leftblock.isEmpty()) {
                    return outbatch;
                }
                /** Whenever a new left block came, we have to start the
                 ** scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }

            }
            while (eosr == false) {
                try {
                    if (rcurs == 0 && lcurs == 0 && lblockcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }
                    for (int k = 0; k < leftblock.size(); k++) {
                        Batch leftbatch = leftblock.get(k);
                        for (i = lcurs; i < leftbatch.size(); ++i) {
                            for (j = rcurs; j < rightbatch.size(); ++j) {
                                Tuple lefttuple = leftbatch.get(i);
                                Tuple righttuple = rightbatch.get(j);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if (k == leftblock.size() - 1 && i == leftbatch.size() - 1 && j == rightbatch.size() - 1) {  //case 1 : just done with entire left block
                                            lblockcurs = 0;
                                            lcurs = 0;
                                            rcurs = 0;
                                        } else if (i != leftbatch.size() - 1 && j == rightbatch.size() - 1) {  //case 2: still on same block, just done with a leftbatch
                                            lblockcurs = k;
                                            lcurs = i + 1;
                                            rcurs = 0;
                                        } else if (i == leftbatch.size() - 1 && j != rightbatch.size() - 1) {  //case 3: still on same block, same leftbatch, iterating through rightbatch
                                            lblockcurs = k;
                                            lcurs = i;
                                            rcurs = j + 1;
                                        } else { // case 4: done with the current leftbatch, move on to next
                                            lblockcurs = k + 1;
                                            lcurs = 0;
                                            rcurs = 0;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        lcurs = 0;
                    }
                    lblockcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }

}
