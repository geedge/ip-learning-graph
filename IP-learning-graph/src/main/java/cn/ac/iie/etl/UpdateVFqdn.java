package cn.ac.iie.etl;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.dao.BaseArangoData;
import cn.ac.iie.utils.ArangoDBConnect;
import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class UpdateVFqdn implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(UpdateVFqdn.class);

    private ArrayList<BaseDocument> documentList;

    private static final ArangoDBConnect arangoManger = ArangoDBConnect.getInstance();

    public UpdateVFqdn(ArrayList<BaseDocument> documentList) {
        this.documentList = documentList;
    }

    @Override
    public void run() {
        ArrayList<BaseDocument> docInsert = new ArrayList<>();
        ArrayList<BaseDocument> docUpdate = new ArrayList<>();
        int i = 0;
        try {
            for (BaseDocument newDocument:documentList){
                String key = newDocument.getKey();
                if (!key.equals("")){
                    i += 1;
                    BaseDocument document = BaseArangoData.v_Fqdn_Map.getOrDefault(key, null);
                    if (document != null){
                        Object lastFoundTime = newDocument.getAttribute("LAST_FOUND_TIME");
                        document.addAttribute("LAST_FOUND_TIME",lastFoundTime);
//                        docUpdate.add(document);
                        docInsert.add(document);
                    }else {
                        docInsert.add(newDocument);
                    }
                    if (i >= ApplicationConfig.UPDATE_ARANGO_BATCH){
//                        arangoManger.insertAndUpdate(docInsert,docUpdate,"FQDN");
                        arangoManger.overwrite(docInsert,"FQDN");
                        LOG.info("更新FQDN:"+i);
                        i = 0;
                    }
                }
            }
            if (i != 0){
//                arangoManger.insertAndUpdate(docInsert,docUpdate,"FQDN");
                arangoManger.overwrite(docInsert,"FQDN");
                LOG.info("更新FQDN:"+i);
            }
        }catch (Exception e){
            LOG.error(e.getMessage());
        }

    }
}
