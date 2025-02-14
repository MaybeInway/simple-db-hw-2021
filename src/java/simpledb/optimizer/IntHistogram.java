package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 * 一个类，用于表示基于单个整数字段的固定宽度直方图。
 */
public class IntHistogram {

    private int buckets; // 桶的数量
    private int min; // 当前 field 最小值
    private int max; // 最大值

    private double avg; // 平均每个桶表示的值数量, 其实是一个整数
    private MyGram[] myGrams; // 每一个桶
    private int ntups; // 一共的tuple数量


    public class MyGram {
        private double left; // 当前桶的左边界
        private double right; // 当前桶的右边界
        private double w; // 当前 gram 的宽度
        private int count; // 当前 gram 中包含的tuple数量

        public MyGram(double left, double right, int count) {
            this.left = left;
            this.right = right;
            this.w = right - left;
            this.count = count;
        }

        /**
         * 判断当前gram是否包含该值, gram是左开右闭
         */
        public boolean isInRange(int value) {
            if (value < left || value >= right) {
                return false;
            } else {
                return true;
            }
        }

        @Override
        public String toString() {
            return "MyGram{" +
                    "left=" + left +
                    ", right=" + right +
                    ", w=" + w +
                    ", count=" + count +
                    '}';
        }
    }


    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.avg = (double) (max - min) / buckets;
        this.myGrams = new MyGram[buckets];
        this.ntups = 0;

        if (avg % 1 != 0) {
            avg = (int) (avg + 1);
        }

        double left = min;
        for (int i = 0; i < buckets; i++) {
            double right = left + avg;
            myGrams[i] = new MyGram(left, right, 0);
            left = right;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = binarySearch(v);
        if (index != -1) {
            myGrams[index].count++;
            ntups++;
        }
    }

    private int binarySearch(int v) {
        int left = 0;
        int right = buckets - 1;

        while (left <= right) {
            int mid = (left + right) / 2;
            if (myGrams[mid].isInRange(v)) {
                return mid;
            } else if (myGrams[mid].left > v) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return -1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int index = binarySearch(v);
        MyGram cur;
        if (index != -1) {
            cur = myGrams[index];
        } else {
            cur = null;
        }

        if (op == Predicate.Op.EQUALS) {
            if (cur == null) {
                return 0;
            } else {
                return (cur.count / cur.w) / ntups;
            }
        } else if (op == Predicate.Op.GREATER_THAN) {
            if (v < min) {
                return 1.0;
            } else if (v >= max) {
                return 0.0;
            } else if (cur != null) {
                double res = ((cur.right - v) / cur.w) * (cur.count * 1.0) / (ntups * 1.0);
                for (int i = index + 1; i < buckets; i++) {
                    res += myGrams[i].count * 1.0 / (ntups * 1.0);
                }
                return res;
            }
        } else if (op == Predicate.Op.LESS_THAN) {
            if (v <= min) {
                return 0.0;
            } else if (v > max) {
                return 1.0;
            } else if (cur != null) {
                double res = ((v - cur.left) / cur.w) * (cur.count * 1.0) / (ntups * 1.0);
                for (int i = 0; i < index; i++) {
                    res += myGrams[i].count * 1.0 / (ntups *1.0);
                }
                return res;
            }
        } else if (op == Predicate.Op.NOT_EQUALS) {
            if (cur == null) {
                return 1.0;
            }
            return 1.0 - (cur.count * 1.0 / (ntups * 1.0));
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            if (v <= min) {
                return 1.0;
            } else if (v > max) {
                return 0.0;
            } else if (cur != null) {
                double res = ((cur.right - v + 1) / cur.w) * (cur.count * 1.0) / (ntups * 1.0);
                for (int i = index + 1; i < buckets; i++) {
                    res += myGrams[i].count * 1.0 / (ntups * 1.0);
                }
                return res;
            }
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            if (v < min) {
                return 0.0;
            } else if (v >= max) {
                return 1.0;
            } else if (cur != null) {
                double res = ((v - cur.left + 1) / cur.w) * (cur.count * 1.0) / (ntups * 1.0);
                for (int i = index - 1; i >= 0; i--) {
                    res += myGrams[i].count *1.0 / (ntups * 1.0);
                }
                return res;
            }
        }
        return 0.0;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return avg;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "IntHistogram{" +
                "buckets=" + buckets +
                ", min=" + min +
                ", max=" + max +
                ", avg=" + avg +
                ", myGrams=" + Arrays.toString(myGrams) +
                '}';
    }
}
