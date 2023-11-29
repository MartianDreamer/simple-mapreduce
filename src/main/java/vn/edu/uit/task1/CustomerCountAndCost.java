package vn.edu.uit.task1;

import java.io.Serializable;

public record CustomerCountAndCost(int customerCount, double cost) implements Serializable {
}
