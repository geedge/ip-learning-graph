package cn.ac.iie.test;

import cn.ac.iie.dao.UpdateGraphData;


/**
 * iplearning程序入口
 * @author wlh
 */
public class IpLearningApplicationTest {

    public static void main(String[] args) {

        UpdateGraphData updateGraphData = new UpdateGraphData();
        updateGraphData.updateArango();

    }
}
