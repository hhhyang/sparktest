package com.sparktest.streamingnetflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.util.List;

import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class NetflowBean {

    @JsonProperty("@timestamp")
    String timestamp;
    String host;
    String protocol_desc;
    String input_snmp_desc;
    String output_snmp_desc;
    String tos;
    String src_product_path;
    String src_isp;
    String src_asn;
    String dst_product_path;
    String dst_isp;
    String dst_asn;
    String qos_tag;

    Integer version;
    Long flow_seq_num;
    Integer engine_type;
    Integer engine_id;
    Integer sampling_algorithm;
    Integer sampling_interval;
    Integer flow_records;
    String ipv4_src_addr;
    String ipv4_dst_addr;
    String ipv4_next_hop;
    Integer input_snmp;
    Integer output_snmp;
    Integer in_pkts;
    Integer in_bytes;
    String first_switched;
    String last_switched;
    Integer l4_src_port;
    Integer l4_dst_port;
    Integer tcp_flags;
    Integer protocol;
    Integer src_tos;
    Integer src_as;
    Integer dst_as;
    Integer src_mask;
    Integer dst_mask;

    String src_country_code2;
    String src_country_code3;
    String src_country_name;
    String src_city_name;
    BigDecimal src_latitude;
    BigDecimal src_longitude;
    List<BigDecimal> src_location;
    String dst_country_code2;
    String dst_country_code3;
    String dst_country_name;
    String dst_city_name;
    BigDecimal dst_latitude;
    BigDecimal dst_longitude;
    List<BigDecimal> dst_location;

    String src_area;
    String src_department;
    String src_product;
    String dst_area;
    String dst_department;
    String dst_product;

    BigDecimal collected_delay;
    BigDecimal flow_ruration;
    BigDecimal flow_speed;
    String qos_check;

    @JsonProperty("@version")
    String meta_version;


}
