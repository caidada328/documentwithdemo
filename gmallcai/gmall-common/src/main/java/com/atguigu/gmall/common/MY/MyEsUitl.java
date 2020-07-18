/*import com.caicai.gmall.common.MY.AppContextHolder;
import com.caicai.gmall.common.MY.InitElasticSearchConfig;
import io.searchbox.client.JestClient;

import io.searchbox.action.*;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.core.search.sort.Sort;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.context.annotation.Configuration;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;

public class MyEsUitl {
    private static JestClient jestclient;
    static{
        jestclient = ((InitElasticSearchConfig)AppContextHolder.getBean(InitElasticSearchConfig.class)).getJestClient();
    }

    public static JestResult deleteDocument(String index,String type,String id){
        Delete delete = new Delete.Builder(id)
                .index(index)
                .type(type)
                .build();
        JestResult result = null;
        try {
            result = jestclient.execute(delete);
            System.out.println("deleteDocument" + result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();

        }
        return result;
    }

    public static <T> T getDocument(T object,String index,String type,String id){
        Get get = new Get.Builder(index,id).type(type).build();
        JestResult result = null;
        T o = null;
        try {
            result = jestclient.execute(get);
            o = (T)result.getSourceAsObject(object.getClass());
            for(Method method:o.getClass().getMethods()){
                System.out.println("getDocument == " + method.getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return o;
    }
    public static <T extends Object> Boolean bulkIndex(String indexName,String typeName,List<T>list){
        Bulk.Builder bulk = new Bulk.Builder();
        for(T o : list){
            Index index = new Index.Builder(o).id(o.toString() + " ")
                    .index(indexName).type(typeName).build();
            bulk.addAction(index);
            try {
                BulkResult result = jestclient.execute(bulk.build());
                if(result != null && result.isSucceeded()){
                    System.out.println("ES 批量插入完成");
                }else{
                    System.out.println("========result:{}."+ result.getJsonString());
                    return result.isSucceeded();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    public static <T> Boolean insertOrUpdateDoc(String index,T o,String type,String uniquId){
        boolean flag = false;
        Index.Builder builder = new Index.Builder(o);
        builder.id(uniquId)
                .refresh(true);
        Index indexDoc = builder.index(index).type(type).build();
        JestResult result;

        try {
            result = jestclient.execute(indexDoc);
            if(result != null && result.isSucceeded()) {
               flag = true;
            }
            System.out.println("==============result:{}." + result.getJsonString());
        } catch (IOException e) {
            System.out.println("ESJestClient insertDoc exception" + e);
        }
        return flag;

    }

    public static JestResult dleteDocByQuery(String index,String type,Map<String,Object> seaarchParams){
        JestResult result  = null;
        BoolQueryBuilder filterQueryBuilders = QueryBuilders.boolQuery();
        for(Map.Entry<String,Object> entry : seaarchParams.entrySet()){
            filterQueryBuilders.must(QueryBuilders.termQuery(entry.getKey(),entry.getValue()));
        }
        DeleteByQuery deleteByQuery = new DeleteByQuery
                .Builder(new SearchSourceBuilder()
                .query(filterQueryBuilders).toString())
                .addIndex(index)
                .addType(type)
                .setParameter("scroll_size", 5000)
                .refresh(true)
                .setParameter("slice",5)
                .build();

        try {
            result = jestclient.execute(deleteByQuery);


        } catch (IOException e) {
            e.printStackTrace();
        }
       return result;

        }

        public static <T> List<T> queryRecords(String index,Class<T> clz,String type,Map<String,Object> searchParams){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder filterQueryBuiders = QueryBuilders.boolQuery();
        for(Map.Entry<String,Object> entry : searchParams.entrySet()){
            filterQueryBuiders.must(QueryBuilders.termQuery(entry.getKey(),entry.getValue()));
        }
        Sort sort = new Sort("gmtCreate",Sort.Sorting.DESC);
        searchSourceBuilder.postFilter(filterQueryBuiders);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(index)
                .addType(type)
                .build();

            try {
                JestResult result = jestclient.execute(search);
                if(result != null && result.isSucceeded()){
                    return result.getSourceAsObjectList(clz );
                }
                System.out.println("=========>result:{}" + result.getJsonString());
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
            return null;
        }
    }*/

