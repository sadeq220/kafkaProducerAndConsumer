package ir.sadeqcloud.stream.utils;


import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Class that acts as a priority queue but has a fixed size.
 * When the maximum number of elements is reached the lowest/highest element
 * will be removed.
 */
public class FixedSizePriorityQueue<T extends SetBaseCompliance> {
    private TreeSet<T> inner;
    private int maxSize;

    public FixedSizePriorityQueue(Comparator<T> comparator, int maxSize) {
        this.inner = new TreeSet<>(comparator);
        this.maxSize = maxSize;
    }
    public FixedSizePriorityQueue(int maxSize){
        this.maxSize=maxSize;
        inner=new TreeSet();
    }

    public FixedSizePriorityQueue<T> add(T element) {
        inner.add(element);
        if (inner.size() > maxSize) {
            inner.pollLast();
        }
        return this;
    }

    public FixedSizePriorityQueue<T> remove(T element) {
        if (inner.contains(element)) {
            inner.remove(element);
        }
        return this;
    }

    public Iterator<T> iterator() {
        return inner.iterator();
    }

    @Override
    public String toString() {
        return "FixedSizePriorityQueue{" +
                "QueueContents:" + inner+"}";

    }

    @JsonGetter("Domains")
    private TreeSet<T> getInner(){
        return inner;
    }

    @JsonSetter("Domains")
    private void setInner(TreeSet<T> set){
        this.inner=set;
    }
}
