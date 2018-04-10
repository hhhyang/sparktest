package com.sparktest.streamingnetflow;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NetflowHandler implements FlatMapFunction<Iterator<String>, String> {

    final String DBURL = "jdbc:mysql://10.242.108.26:3306/iplib?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    final String DBUSER = "root";
    final String DBPASS = "abc123";

    private String getPdb(PreparedStatement pstmt, String ip, Integer port) {

        String pdb = null;
        ResultSet rs = null;
        try {
            pstmt.setString(1, ip + "_" + port);

            rs = pstmt.executeQuery();
            if (rs.next()) {
                pdb = rs.getString("pdb");
                //System.out.println("pdb: " + pdb);
            }
            else {
                rs.close();

                pstmt.setString(1, ip);

                rs = pstmt.executeQuery();
                if (rs.next()) {
                    pdb = rs.getString("pdb");
                    //System.out.println("pdb: " + pdb);
                }
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            if (rs != null) {
                try {
                    rs.close();
                }
                catch (Exception e) {

                }
            }

        }

        return pdb;

    }

    @Override
    public Iterable<String> call(Iterator<String> stringIterator) throws Exception {
        List<String> list = new ArrayList<String>();
        ObjectMapper mapper = new ObjectMapper();

        SqlConnectionManager manager = new SqlConnectionManager(DBURL, DBUSER, DBPASS);

        String sql = "SELECT pdb FROM ip_port_pdb where ip = ?";

        PreparedStatement pstmt = manager.setPrepareStatement(sql);
        if (pstmt == null) {
            System.out.println("get PreparedStatement failed");
            return list;
        }

        while (stringIterator.hasNext()) {

            String s = stringIterator.next();
            NetflowBean netflowBean = mapper.readValue(s, NetflowBean.class);
            //查表
            String pdb = getPdb(pstmt, netflowBean.getIpv4_src_addr(), netflowBean.getL4_src_port());
            if (pdb != null) {
                System.out.println("src ip: " + netflowBean.getIpv4_src_addr()
                    + " port: " + netflowBean.getL4_src_port() + " pdb: " + pdb);
                netflowBean.setSrc_product_path(pdb);
            }

            pdb = getPdb(pstmt, netflowBean.getIpv4_dst_addr(), netflowBean.getL4_dst_port());
            if (pdb != null) {
                System.out.println("dst ip: " + netflowBean.getIpv4_dst_addr()
                    + " port: " + netflowBean.getL4_dst_port() + " pdb: " + pdb);
                netflowBean.setDst_product_path(pdb);
            }

            String result = mapper.writeValueAsString(netflowBean);
            System.out.println("log: " + result);
            list.add(result);
        }

        manager.close();
        return list;
    }
}
