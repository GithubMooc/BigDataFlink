package com.atguigu.flink.chapter05;
// 对流重新分区
// KeyBy：先按照key分组, 按照key的双重hash来选择后面的分区
// shuffle：对流中的元素随机分区
// reblance：对流中的元素平均分布到每个区.当处理倾斜数据的时候, 进行性能优化
// rescale：同 rebalance一样, 也是平均循环的分布数据。但是要比rebalance更高效, 因为rescale不需要通过网络, 完全走的"管道"。
public class Demo23 {
    public static void main(String[] args) {
    }
}

