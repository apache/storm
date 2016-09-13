package org.apache.storm.trident.operation.impl;

import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class JoinState {
    List<List>[] sides;
    int numSidesReceived = 0;
    int[] indices;
    TridentTuple group;

    public JoinState(int numSides, TridentTuple group) {
        sides = new List[numSides];
        indices = new int[numSides];
        this.group = group;
        for(int i=0; i<numSides; i++) {
            sides[i] = new ArrayList<List>();
        }
    }
}
