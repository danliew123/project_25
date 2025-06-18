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

    // QH
    private File f;
    private TupleDesc td;
    private int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *          the file that stores the on-disk backing store for this heap
     *          file.
     */
    // QH
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.tableId = f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    // QH
    public File getFile() {
        // some code goes here
        return f;
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
    // QH
    public int getId() {
        // some code goes here
        return tableId;
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    //QH
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
        // throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    // QH
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            int pageNumber = pid.getPageNumber();
            int pageSize = BufferPool.getPageSize(); // bytes per page, including header
            long offset = pageNumber * pageSize;

            RandomAccessFile raf = new RandomAccessFile(f, "r");
            if ((long) (pageNumber + 1) * pageSize > raf.length()) {
                raf.close();
                throw new IllegalArgumentException("Page number out of bounds");
            }

            byte[] data = new byte[pageSize]; // amount of info to read
            raf.seek(offset); // find starting position of page
            raf.readFully(data); // read exactly pageSizes bytes into memory
            raf.close();

            return new HeapPage((HeapPageId) pid, data);

        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read page", e);
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    //QH
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            private int pageIndex = 0;
            private Iterator<Tuple> tupleIterator = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageIndex = 0;
                loadPageTuples();
            }

            private void loadPageTuples() throws TransactionAbortedException, DbException{
                if (pageIndex < numPages()) {
                    HeapPageId pid = new HeapPageId(tableId, pageIndex);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    tupleIterator = page.iterator();
                } else {
                    tupleIterator = null;
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (tupleIterator == null) return false;
                // if no more tuples to iterate over OR if the page is empty THEN go to the next page
                while (!tupleIterator.hasNext()) {
                    pageIndex++;
                    if (pageIndex >= numPages()) return false;
                    loadPageTuples();
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) throw new NoSuchElementException();
                return tupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                tupleIterator = null;
            }

        };
    }

}
