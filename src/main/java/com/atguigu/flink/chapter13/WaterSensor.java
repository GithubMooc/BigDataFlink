package com.atguigu.flink.chapter13;

import lombok.*;

/**
 * @author Master
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
