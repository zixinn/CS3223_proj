/**
 * To group the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

public class GroupBy extends Operator {

    Operator base;                      // Base operator
    ArrayList<Attribute> attrset;       // Set of atttributes to project
    ArrayList<Integer> attrIndex;       // Index of attributes to project
    ArrayList<Attribute> groupbylist;   // Set of atttributes to groupby
    ArrayList<Integer> groupbyIndex;    // Index of attributes to group by
    int batchsize;                      // Number of tuples per out batch
    int numbuff;                        // Number of buffers available
    int curs;                           // Cursor for input buffer
    boolean eos;                        // Whether end of stream is reached
    Sort sort;                          // Sort
    Tuple prevtuple;                    // Previous tuple
    Batch inbatch;                      // Buffer page for input
    Batch outbatch;                     // Buffer page for output

    public GroupBy(Operator base, ArrayList<Attribute> as, ArrayList<Attribute> groupbylist, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.groupbylist = groupbylist;
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
     * * grouped by and projected from the base operator
     * * and sort the table on the attributes to group by
     **/
    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        sort = new Sort(base, groupbylist, numbuff, true);

        Schema baseSchema = base.getSchema();
        attrIndex = new ArrayList<>(attrset.size());
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);
            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggregation is not implemented.");
                System.exit(1);
            }
            attrIndex.add(baseSchema.indexOf(attr.getBaseAttribute()));
        }

        groupbyIndex = new ArrayList<>(groupbylist.size());
        for (int i = 0; i < groupbylist.size(); i++) {
            Attribute attr = attrset.get(i);
            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggregation is not implemented.");
                System.exit(1);
            }
            groupbyIndex.add(baseSchema.indexOf(attr));
        }

        curs = 0;
        eos = false;
        prevtuple = null;
        
        return sort.open();
    }

    /**
     * Read next tuple from sorted relation 
     * * and project relevant attributes
     */
    public Batch next() {
        if (eos) {
            close();
            return null;
        }

        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
            if (curs == 0) {
                inbatch = (Batch) sort.next();
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            for (int i = curs; i < inbatch.size(); i++) {
                Tuple tuple = inbatch.get(i);
                ArrayList<Object> present = new ArrayList<>();

                if (prevtuple != null && Tuple.compareTuples(prevtuple, tuple, groupbyIndex, groupbyIndex) == 0) { // same group
                    if (Tuple.compareTuples(prevtuple, tuple, attrIndex, attrIndex) != 0) {
                        System.err.println("Group by: Error in projecting results");
                        System.exit(1);
                    }
                    prevtuple = tuple;
                } else {
                    prevtuple = tuple;
                    for (int j = 0; j < attrIndex.size(); j++) {
                        Object data = tuple.dataAt(attrIndex.get(j));
                        present.add(data);
                    }
                    Tuple outtuple = new Tuple(present);
                    outbatch.add(outtuple);
                    if (outbatch.isFull()) {
                        if (i == inbatch.size() - 1) {
                            curs = 0;
                        } else {
                            curs = i + 1;
                        }
                        return outbatch;
                    }
                }
            }
            curs = 0;
        }
        
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        sort.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        ArrayList<Attribute> newgroupbylist = new ArrayList<>();
        for (int i = 0; i < groupbylist.size(); ++i)
            newgroupbylist.add((Attribute) groupbylist.get(i).clone());
        GroupBy newgroupby = new GroupBy(newbase, newattr, newgroupbylist, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newgroupby.setSchema(newSchema);
        return newgroupby;
    }
}
