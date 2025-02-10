package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

public class AggregateIter implements OpIterator{

    private Map<Field, List<Field>> group;
    private int gbfield;
    private Type gbfieldtype;
    private Aggregator.Op what;

    private Iterator<Tuple> tupleIterator;
    private TupleDesc tupleDesc; // 如果有 groupby 就是两条，否则就是一条
    private List<Tuple> resultSet; // 存储聚合后的结果数据

    public AggregateIter(Map<Field, List<Field>> group, int gbfield, Type gbfieldtype, Aggregator.Op what) {
        this.group = group;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        if (gbfield != Aggregator.NO_GROUPING) {
            Type[] types = new Type[2];
            types[0] = gbfieldtype;
            types[1] = Type.INT_TYPE;
            this.tupleDesc = new TupleDesc(types);
        } else {
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            this.tupleDesc = new TupleDesc(types);
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.resultSet = new ArrayList<>();
        if (what == Aggregator.Op.COUNT) {
            for (Field field : group.keySet()) {
                Tuple tuple = new Tuple(tupleDesc);
                if (gbfield != Aggregator.NO_GROUPING) {
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(group.get(field).size()));
                } else {
                    tuple.setField(0, new IntField(group.get(field).size()));
                }
                resultSet.add(tuple);
            }
        } else if (what == Aggregator.Op.MIN) {
            for (Field field : group.keySet()) {
                int min = Integer.MAX_VALUE;
                for (Field field1 : group.get(field)) {
                    min = Math.min(min, ((IntField) field1).getValue());
                }

                Tuple tuple = new Tuple(tupleDesc);
                if (gbfield != Aggregator.NO_GROUPING) {
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(min));
                } else {
                    tuple.setField(0, new IntField(min));
                }
                resultSet.add(tuple);
            }
        } else if (what == Aggregator.Op.MAX) {
            for (Field field : group.keySet()) {
                int max = Integer.MIN_VALUE;
                for (Field field1 : group.get(field)) {
                    max = Math.max(max, ((IntField) field1).getValue());
                }
                Tuple tuple = new Tuple(tupleDesc);
                if (gbfield != Aggregator.NO_GROUPING) {
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(max));
                } else {
                    tuple.setField(0, new IntField(max));
                }
                resultSet.add(tuple);
            }
        } else if (what == Aggregator.Op.AVG) {
            for (Field field : group.keySet()) {
                int sum = 0;

                for (Field field1 : group.get(field)) {
                    sum += ((IntField) field1).getValue();
                }

                int avg = sum / group.get(field).size();
                Tuple tuple = new Tuple(tupleDesc);
                if (gbfield != Aggregator.NO_GROUPING) {
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(avg));
                } else {
                    tuple.setField(0, new IntField(avg));
                }
                resultSet.add(tuple);
            }
        } else if (what == Aggregator.Op.SUM) {
            for (Field field : group.keySet()) {
                int sum = 0;
                for (Field field1 : group.get(field)) {
                    sum += ((IntField) field1).getValue();
                }
                Tuple tuple = new Tuple(tupleDesc);
                if (gbfield != Aggregator.NO_GROUPING) {
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(sum));
                } else {
                    tuple.setField(0, new IntField(sum));
                }
                resultSet.add(tuple);
            }
        }
        this.tupleIterator = resultSet.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (tupleIterator == null) {
            return false;
        }
        return tupleIterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return tupleIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if (resultSet != null) {
            this.tupleIterator = resultSet.iterator();
        }
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public void close() {
        this.tupleIterator = null;
    }
}
