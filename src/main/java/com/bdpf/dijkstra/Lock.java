package com.bdpf.dijkstra;

public class Lock {
    private boolean value = false;

    public boolean isLocked() {
        return this.value;
    }

    public void lock() {
        this.value = true;
    }

    public void unlock() {
        this.value = false;
    }

}
