package com.company;


import java.util.LinkedList;

/**
 * Created by mialiu on 11/5/15.
 */
public class TaskQueue<E> {

    private LinkedList<E> _list;
    private int _maxlen;
    private boolean _closed;

    public TaskQueue(int len) {
        _list = new LinkedList<E>();
        _maxlen = len;
        _closed = false;
    }

    public synchronized boolean Put(E element) {
        if (_closed) {
            return false;
        }
        if (_list.size() >= _maxlen) {
            return false;
        }
        _list.add(element);
        this.notify();
        //System.out.println("put>>");
        return true;
    }

    public synchronized E Get() {
        if (_closed) {
            return null;
        }

        if (_list.size() <= 0) {
            try {
                //System.out.println("waiting...");
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //System.out.println("wakeup!");
        }

        if (_closed) {
            return null;
        }

        return _list.poll();
    }

    public synchronized void Close() {
        _closed = true;
        this.notifyAll();
    }
}
