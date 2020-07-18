///*import com.caicai.gmall.common.MY.AppContextHolder;
//import com.caicai.gmall.common.MY.InitElasticSearchConfig;
//import io.searchbox.client.JestClient;
//
//import io.searchbox.action.*;
//import io.searchbox.client.JestResult;
//import io.searchbox.core.*;
//import io.searchbox.core.search.sort.Sort;
//import org.elasticsearch.index.query.BoolQueryBuilder;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.springframework.context.annotation.Configuration;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//
//import java.io.IOException;
//import java.lang.reflect.Method;
//import java.util.List;
//import java.util.Map;
//
//
//
////import com.rongchu.saas.common.utils.AppContextHolder;
////import com.rongchu.saas.core.pojo.BasePO;
////import com.rongchu.saas.elasticsearch.config.InitElasticSearchConfig;
////import com.rongchu.saas.elasticsearch.esPage.ElasticSearchPage;
////import com.rongchu.saas.elasticsearch.esPage.PageRequest;
//
////import lombok.extern.slf4j.Slf4j;
//
//
//
//@Configuration
//public class EsUtil3 {
//    private static JestClient jestClient;
//    static{
//        jestClient=
//                ((InitElasticSearchConfig)AppContextHolder.getBean(InitElasticSearchConfig.class)).getJestClient();}
//    /** * 根据主键删除文档
//     * * @param index 待操作的库 *
//     * @param type 待操作的表 *
//     * @param id 待操作的主键id *
//     * @return */
//    public static JestResult deleteDocument(String index, String type, String id) {
//        Delete delete = new Delete.
//                Builder(id).
//                index(index).
//                type(type).
//                build();
//        JestResult result = null ;
//        try {
//            result = jestClient.execute(delete);
//            log.info("deleteDocument == " + result.getJsonString());
//       }
//        catch (IOException e)
//        { e.printStackTrace(); }
//        return result; }
//
//
//
///** * 根据查询条件
// * * @param index 待操作的库
// * * @param type 待操作的表 *
// * @param searchParams 查询map *
// * @return */
//public static JestResult deleteDocByQuery(String index, String type, Map<String,Object> searchParams) {
//    JestResult result = null ;
//    BoolQueryBuilder filterQueryBuilders = QueryBuilders.boolQuery();
//    for (Map.Entry<String,Object> entry : searchParams.entrySet()) {
//        filterQueryBuilders.must(QueryBuilders.termQuery(entry.getKey(),entry.getValue()));
//    }
//    DeleteByQuery deleteByQuery = new DeleteByQuery
//        .Builder(new SearchSourceBuilder()
//        .query(filterQueryBuilders).toString())
//        .addIndex(index)
//        .addType(type)
//        .setParameter("scroll_size", 5000)
//        .refresh(true)
//        .setParameter("slices", 5)
//        .build();
//    try {
//        result = jestClient.execute(deleteByQuery);
//        log.info("del docs == " + result.getJsonString());
//    } catch (IOException e) { e.printStackTrace(); }
//    return result; }
///** * 根据主键获取文档
// *  * @param object 返回对象 *
// *  @param index 待操作的库 *
// *  @param type 待操作的表 *
// *  @param id 待操作的主键id *
// *  @return */
//public static <T> T getDocument(T object , String index, String type, String id) {
//    Get get = new Get.Builder(index, id).
//        type(type).build();
//    JestResult result = null ;
//    T o = null;
//    try {
//        result = jestClient.execute(get);
//        o = (T) result.getSourceAsObject(object.getClass());
//        for (Method method : o.getClass().getMethods()) {
//            log.info("getDocument == " + method.getName()); }
//    }catch (IOException e) { e.printStackTrace(); }
//    return o; }
///** * 批量存储 *
// * @param indexName *
// * @param typeName *
// * @param list */
//public static <T extends BasePO> Boolean bulkIndex(String indexName, String typeName , List<T> list) {
//    Bulk.Builder bulk = new Bulk.Builder();
//    for(T o : list) {
//        Index index = new Index.
//        Builder(o).
//        id(o.getId()+"").
//        index(indexName).
//        type(typeName).build();
//        bulk.addAction(index); }
//    try {
//        BulkResult result = jestClient.execute(bulk.build());
//        if (result != null && result.isSucceeded()){
//            log.info("ES 批量插入完成");
//        }else{
//            log.error("=====================>result:{}.",result.getJsonString());
//        }return result.isSucceeded(); }
//    catch (IOException e) { e.printStackTrace(); log.error(e.getMessage()); }
//    return false; }
///**
// * * 新增或者更新文档 *
// * @param o *
// * @param type *
// * @param uniqueId *
// * @param <T> *
// * @return */
//public static <T> Boolean insertOrUpdateDoc(String index,T o, String type,String uniqueId) {
//    //是否插入成功标识
//       boolean flag = false;
//       Index.Builder builder = new Index.Builder(o);
//       builder.id(uniqueId);
//       builder.refresh(true);
//       Index indexDoc = builder.index(index).type(type).build();
//       JestResult result;
//       try {
//           result = jestClient.execute(indexDoc);
//           if (result != null && result.isSucceeded()){
//               return Boolean.TRUE; } log.error("=====================>result:{}.",result.getJsonString());
//       } catch (IOException e) {
//           log.error("ESJestClient insertDoc exception", e);
//       }
//       return flag;
//}
//    // /** * 查找记录列表 * @param type * @param searchParams * @param <T> * @return */
//    public static <T> List<T> queryRecords(String index, Class<T> clz, String type, Map<String,Object> searchParams) {
//    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//    BoolQueryBuilder filterQueryBuilders = QueryBuilders.boolQuery();
//    for (Map.Entry<String,Object> entry : searchParams.entrySet()) {
//        filterQueryBuilders.must(QueryBuilders.termQuery(entry.getKey(),entry.getValue())); }
//    Sort sort = new Sort("gmtCreate",Sort.Sorting.DESC); searchSourceBuilder.postFilter(filterQueryBuilders);
//    /*
//    Search search = new Search.Builder(searchSourceBuilder.toString()) .
//    addIndex(index).
//    addType(type).
//    addSort(sort).
//    build();*/
//    Search search = new Search.Builder(searchSourceBuilder.toString()) .
//        addIndex(index).
//        addType(type).
//        build();
//    try {
//        JestResult result = jestClient.execute(search);
//        if (result != null && result.isSucceeded()){
//            return result.getSourceAsObjectList(clz); } log.error("=====================>result:{}.",result.getJsonString());
//    } catch (IOException e) {
//        log.error(e.getMessage()); e.printStackTrace();
//    } return null;
//}
//        }
//    /** * 分页查找记录 * @param ty
//
