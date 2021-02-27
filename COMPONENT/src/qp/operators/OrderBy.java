package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class OrderBy extends Operator {

    private Operator base;                      // Base Operator
    private int batchsize;                      // Number of tuples per out batch
    private ArrayList<Attribute> attrset;       // Set of attributes to project
    private ArrayList<Attribute> compareAttri;  // Set of attributes to orderBy
    private int numBuff;                        // Number of buffers available
    private boolean isAscending;                // Sort ascending or descending
    
    Batch inbatch;                              // Buffer for input
    Batch outbatch;                             // Buffer for output
    int curs;                                   // Cursor to keep track of which file we at
    Sort sort;

    public OrderBy(Operator base, ArrayList<Attribute> attrset,  ArrayList<Attribute> compareAttri, boolean isAscending) {
        super(OpType.SORT);
        this.attrset = attrset;
        this.isAscending = isAscending;
        this.base = base;
        this.compareAttri = compareAttri;
    }

    public boolean getOrderType() {
        return isAscending;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }


    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    /**
     * Sorts the table from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        sort = new Sort(base, compareAttri, numBuff, isAscending);
        if (!sort.open()) {
            System.out.println("Unable to open sort");
            return false;
        }
        
        inbatch = sort.next();
        curs = 0;
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        if (inbatch == null) {
            return null;
        }
        
        while (!outbatch.isFull()) {
            if (curs >= inbatch.size()) {
                inbatch = sort.next();
                curs = 0;
                if (inbatch == null) {
                    break;
                }
            } else {
                outbatch.add(inbatch.get(curs));
                curs++;
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        sort.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();

        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        OrderBy newOrderBy = new OrderBy(newbase, newattr, this.compareAttri, isAscending);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newOrderBy.setSchema(newSchema);
        return newOrderBy;
    }
}
