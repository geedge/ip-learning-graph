package cn.ac.iie.test;

import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.dao.UpdateGraphData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IpLearningApplicationTest {
    private static final Logger LOG = LoggerFactory.getLogger(IpLearningApplicationTest.class);

    public static void main(String[] args) {
        LOG.info("Ip Learning Application开始运行");
        BaseArangoData baseArangoData = new BaseArangoData();
        baseArangoData.baseDocumentDataMap();

        LOG.info("历史数据读取完成,开始更新数据");
        UpdateGraphData updateGraphData = new UpdateGraphData();
        updateGraphData.updateArango();

    }
}
