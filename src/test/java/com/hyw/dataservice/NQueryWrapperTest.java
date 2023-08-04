package com.hyw.dataservice;

import com.hyw.dataservice.dto.FieldAttr;

public class NQueryWrapperTest {

    public static void main(String[] args){

        GetDataWrapper qw = new GetDataWrapper<FieldAttr>();

        System.out.println(qw.getEntity());
    }
}
