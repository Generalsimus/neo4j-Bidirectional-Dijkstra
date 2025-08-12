package com.bdpf.dijkstra;

import java.util.*;
 

public class Linker<E> implements Iterable<E> {
    public Linker<E> before; // Points to the previous node
    public E element; // The value of the current node

    private int size = 0;

    // Default constructor for an empty linker
    public Linker() {
    }

    // Constructor for a linker with an initial element
    public Linker(E element) {
        this.element = element;
        this.size = 1;
    }

    // Private constructor for internal usage (creating a new node with a link to
    // the previous node)
    private Linker(E element, Linker<E> before, int size) {
        this.element = element;
        this.before = before;
        this.size = size;
    }

    // Adds a new element by creating a new Linker node and linking it to the
    // current one
    public Linker<E> push(E element) {
        return new Linker<>(element, this, this.size + 1);
    }

    // Returns the current size of the linked list
    public int getSize() {
        return this.size;
    }

    @Override
    public Iterator<E> iterator() {
        return new LinkerIterator(this);
    }

    // Iterator class to traverse the Linker nodes
    private class LinkerIterator implements Iterator<E> {
        private Linker<E> current;

        // Initialize with the starting node
        public LinkerIterator(Linker<E> start) {
            this.current = start;
        }

        @Override
        public boolean hasNext() {
            return current != null && current.element != null;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            E element = current.element;
            current = current.before; // Move to the previous node
            return element;
        }
    }
}