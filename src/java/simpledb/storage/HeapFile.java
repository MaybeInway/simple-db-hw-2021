package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPage heapPage = null;
        int pageSize = BufferPool.getPageSize();
        byte[] buf = new byte[pageSize];

        try {

            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek((long) pid.getPageNumber() * pageSize);

            if (randomAccessFile.read(buf) == -1) {
                return null;
            }

            heapPage = new HeapPage((HeapPageId) pid, buf);
            randomAccessFile.close();
        }   catch (FileNotFoundException e) {
            e.printStackTrace();
        }   catch (IOException e) {
            e.printStackTrace();
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        HeapPageId pageId = (HeapPageId) page.getId();
        int size = BufferPool.getPageSize();
        int pageNum = pageId.getPageNumber();
        byte[] pageData = page.getPageData();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek((long) pageNum * size);
        randomAccessFile.write(pageData);
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        if (!getFile().canRead() || !getFile().canWrite()) {
            throw new DbException("file can not read or write");
        }
        List<Page> res = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pageId = new HeapPageId(getId(), i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (heapPage == null) {
                Database.getBufferPool().unsafeReleasePage(tid, pageId);
                continue;
            }
            if (heapPage.getNumEmptySlots() == 0) {
                Database.getBufferPool().unsafeReleasePage(tid, pageId);
                continue;
            }
            heapPage.insertTuple(t);
            heapPage.markDirty(true, tid); // page在修改后设置为脏页
            res.add(heapPage);
            return res;
        }
        // 新建一个page
        HeapPageId pageId = new HeapPageId(getId(), numPages());
        HeapPage heapPage = new HeapPage(pageId, HeapPage.createEmptyPageData());
        heapPage.insertTuple(t);
        writePage(heapPage);
        res.add(heapPage);
        return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> res = new ArrayList<>();
        HeapPageId pageId = (HeapPageId) t.getRecordId().getPageId();
        // 获取heapPage
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        if (heapPage == null) {
            throw new DbException("heapPage is null");
        }
        heapPage.deleteTuple(t);
        res.add(heapPage);
        return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, Permissions.READ_ONLY);
    }

    /**
     * 定义一个内部类， 实现一个迭代器， 遍历文件中的Tuple，但是不要一次性读取所有的Tuple，而是一页一页的读取遍历
     */
    public class HeapFileIterator implements DbFileIterator {

        private TransactionId tid; // 事务ID
        private Permissions permissions; // 权限
        BufferPool bufferPool = Database.getBufferPool();
        private Iterator<Tuple> iterator; // 这是每一页的迭代器

        int num = 0;

        public HeapFileIterator(TransactionId tid, Permissions permissions) {
            this.tid = tid;
            this.permissions = permissions;
        }

        /**
         * 开始遍历，默认从第一页开始
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            num = 0;
            HeapPageId heapPageId = new HeapPageId(getId(), num);
            HeapPage page = (HeapPage)this.bufferPool.getPage(tid, heapPageId, permissions);

            if (page == null) {
                throw new DbException("page is null");
            } else {
                iterator = page.iterator();
            }
        }

        /**
         * 获取下一个有数据的页
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public boolean nextPage() throws DbException, TransactionAbortedException {
           while (true) {
               num++;
               if (num >= numPages()) {
                   return false;
               } else {
                   HeapPageId heapPageId = new HeapPageId(getId(), num);
                   HeapPage page = (HeapPage)this.bufferPool.getPage(tid, heapPageId, permissions);
                   if (page == null) {
                       continue;
                   }
                   iterator = page.iterator();
                   if (iterator.hasNext()) {
                       return true;
                   }
               }
           }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null) {
                return false;
            }
            if (iterator.hasNext()) {
                return true;
            } else {
                return nextPage();
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}

