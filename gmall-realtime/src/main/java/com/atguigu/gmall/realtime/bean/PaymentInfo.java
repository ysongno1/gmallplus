package com.atguigu.gmall.realtime.bean;


import lombok.Data;
import java.math.BigDecimal;

@Data
public class PaymentInfo {

    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String payment_type;
    String subject;
    String create_time;
    String callback_time;

}
