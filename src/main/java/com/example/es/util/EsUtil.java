package com.example.es.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.example.es.annotation.EsField;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * <p>
 *
 * </p>
 *
 * @author heshuyao
 * @since 2021/8/3 - 16:39
 */
@Slf4j
@AllArgsConstructor
public class EsUtil {

    private final RestHighLevelClient restHighLevelClient;

    private static RestHighLevelClient client;

    @PostConstruct
    public void init() {
        client = restHighLevelClient;
    }

    /**
     * <p>
     * 获取SearchResponse对象
     * </p>
     *
     * @param searchRequest searchRequest
     * @return SearchResponse
     * @author heshuyao
     * @since 2021/8/3
     */
    public static SearchResponse search(SearchRequest searchRequest) {
        try {
            return client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("esUtil# search#" + e.getMessage(), e);
            return null;
        }
    }

    /**
     * <p>
     * 将传入对象中要存入es的字段和属性进行转换
     * </p>
     *
     * @param source source
     * @param extra  extra  所需要加入的额外字段
     * @return Map
     * @author heshuyao
     * @since 2021/7/30
     */
    public static Map<String, Object> toEsMap(Object source, Consumer<? super Map<String, Object>> extra) {
        Map<String, Object> esMap = new HashMap<>(11);
        Field[] fields = ReflectUtil.getFields(source.getClass());
        Arrays.stream(fields).forEach(field -> {
            field.setAccessible(true);
            EsField esFiled = field.getAnnotation(EsField.class);
            if (esFiled != null && !esFiled.isExist()) {
                return;
            }
            String filedName = esFiled != null ? esFiled.name() : field.getName();
            try {
                Object filedValue = field.get(source);
                if (filedValue != null && filedValue.getClass().equals(LocalDateTime.class)) {
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    filedValue = dtf.format(Convert.convert(LocalDateTime.class, filedValue));
                }
                esMap.put(filedName, filedValue);
            } catch (IllegalAccessException e) {
                log.error("esUtil#toEsMap#" + e.getMessage(), e);
            }
        });
        if (extra != null) {
            extra.accept(esMap);
        }
        return esMap;
    }

    /**
     * <p>
     * 将传入对象中要存入es的字段和属性进行转换
     * </p>
     *
     * @param list list
     * @return List
     * @author heshuyao
     * @since 2021/8/2
     */
    public static List<Map<String, Object>> toEsMaps(List<? extends Object> list) {
        if (CollUtil.isNotEmpty(list)) {
            return list.stream().map(a -> EsUtil.toEsMap(a, null)).collect(Collectors.toList());
        }
        return null;
    }

    /**
     * <p>
     * 增添join类型的子文档
     * </p>
     *
     * @param indexName indexName
     * @param joinName  joinName
     * @param childName childName
     * @param parentId  parentId
     * @param sourceMap sourceMap
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/31
     */
    public static Boolean addChildDoc(String indexName, String joinName, String childName, Integer parentId,
                                      Map<String, Object> sourceMap) {
        if (checkChildDoc(indexName, Convert.toInt(sourceMap.get("id")), parentId)) {
            return Boolean.FALSE;
        }
        sourceMap.put(joinName, JSONUtil.parseObj(StrUtil.format("{name:{},parent:{}}", childName, parentId)));
        IndexRequest indexRequest = new IndexRequest(indexName, "_doc");
        indexRequest.id(String.valueOf(sourceMap.remove("id")))
                .routing(String.valueOf(parentId))
                .source(sourceMap, XContentType.JSON);
        try {
            client.index(indexRequest, RequestOptions.DEFAULT);
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error("esUtil # addChildDoc #" + e.getMessage(), e);
            return Boolean.FALSE;
        }
    }

    /**
     * <p>
     * 批量增加子文档
     * </p>
     *
     * @param indexName     indexName
     * @param joinName      joinName
     * @param childName     childName
     * @param parentKeyName parentKeyName  join类型文档中父文档与子文档关联的主外键名称,
     * @param sourceMaps    sourceMaps
     * @return Boolean
     * @author heshuyao
     * @since 2021/8/1
     */
    public static Boolean batchAddChild(String indexName, String joinName, String childName, String parentKeyName,
                                        List<? extends Map<String, Object>> sourceMaps) {
        if (CollUtil.isNotEmpty(sourceMaps)) {
            BulkRequest bulkRequest = new BulkRequest();
            int count = sourceMaps.size();
            for (Map<String, Object> m : sourceMaps) {
                Object id = m.get("id");
                Integer parentId = Convert.toInt(m.get(parentKeyName));
                if (checkChildDoc(indexName, Convert.toInt(id), parentId) || ObjectUtil.isEmpty(id)) {
                    count--;
                    continue;
                }
                IndexRequest indexRequest = new IndexRequest(indexName);
                m.put(joinName, JSONUtil.parseObj(StrUtil.format("{name:{},parent:{}}", childName, parentId)));
                indexRequest.type("_doc")
                        .id(String.valueOf(m.remove("id")))
                        .routing(String.valueOf(parentId))
                        .source(m, XContentType.JSON)
                        .opType("create");
                bulkRequest.add(indexRequest);
            }
            if (count == 0) {
                return Boolean.FALSE;
            }
            try {
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                return Boolean.TRUE;
            } catch (IOException e) {
                log.error("esUtil # batchAddChild #" + e.getMessage(), e);
                return Boolean.FALSE;
            }
        }
        return Boolean.FALSE;
    }

    /**
     * <p>
     * 删除join类型的子文档
     * </p>
     *
     * @param indexName indexName
     * @param childId   childId
     * @param parentId  parentId
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean delChildDoc(String indexName, Integer childId, Integer parentId) {
        if (!checkChildDoc(indexName, childId, parentId)) {
            return Boolean.FALSE;
        }
        DeleteRequest deleteRequest = new DeleteRequest(indexName, "_doc", String.valueOf(childId))
                .routing(String.valueOf(parentId));
        try {
            client.delete(deleteRequest, RequestOptions.DEFAULT);
            return Boolean.TRUE;
        } catch (IOException e) {
            e.printStackTrace();
            log.error("esUtil # delChildDoc #" + e.getMessage(), e);
            return Boolean.FALSE;
        }
    }

    /**
     * <p>
     * 删除文档
     * </p>
     *
     * @param indexName indexName
     * @param id        id
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean delDoc(String indexName, Integer id) {
        if (!checkDoc(indexName, id)) {
            return Boolean.FALSE;
        }
        DeleteRequest deleteRequest = new DeleteRequest(indexName, "_doc",
                String.valueOf(id));
        try {
            client.delete(deleteRequest, RequestOptions.DEFAULT);
            return Boolean.TRUE;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("esUtil # delDoc #" + e.getMessage(), e);
            return Boolean.FALSE;
        }
    }


    /**
     * <p>
     * 增添join类型的父文档
     * </p>
     *
     * @param indexName indexName
     * @param sourceMap sourceMap
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/30
     */
    public static Boolean addParentDoc(String indexName, String joinName, String parentName, Map<String, Object> sourceMap) {
        sourceMap.put(joinName, parentName);
        return addDoc(indexName, sourceMap);
    }

    /**
     * <p>
     * 增添文档
     * </p>
     *
     * @param indexName indexName
     * @param sourceMap sourceMap
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/31
     */
    public static Boolean addDoc(String indexName, Map<String, Object> sourceMap) {
        if (checkParentDoc(indexName, Convert.toInt(sourceMap.get("id")))) {
            return Boolean.FALSE;
        }
        IndexRequest indexRequest = new IndexRequest(indexName, "_doc");
        indexRequest.id(String.valueOf(sourceMap.remove("id")))
                .source(sourceMap, XContentType.JSON);
        try {
            client.index(indexRequest, RequestOptions.DEFAULT);
            return Boolean.TRUE;
        } catch (IOException e) {
            log.error("esUtil # addDoc # " + e.getMessage(), e);
            return Boolean.FALSE;
        }
    }

    /**
     * <p>
     * 删除父文档
     * </p>
     *
     * @param indexName indexName
     * @param parentId  parentId
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/31
     */
    public static Boolean delParentDoc(String indexName, Integer parentId) {
        List<Integer> childrenIds = getChildrenIds(indexName, "collection_content", parentId);
        if (childrenIds != null) {
            childrenIds.forEach(id -> delChildDoc(indexName, id, parentId));
        }
        return delDoc(indexName, parentId);
    }

    /**
     * <p>
     * 更新join类型的父文档
     * </p>
     *
     * @param indexName indexName
     * @param sourceMap sourceMap
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/30
     */
    public static Boolean updateParentDoc(String indexName, Map<String, Object> sourceMap) {
        return updateDoc(indexName, sourceMap);
    }

    /**
     * <p>
     * 更新文档
     * </p>
     *
     * @param indexName indexName
     * @param sourceMap sourceMap
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/30
     */
    public static Boolean updateDoc(String indexName, Map<String, Object> sourceMap) {
        if (!checkDoc(indexName, Convert.toInt(sourceMap.get("id")))) {
            return Boolean.FALSE;
        }
        UpdateRequest updateRequest = new UpdateRequest(indexName, "_doc",
                String.valueOf(sourceMap.remove("id")));
        updateRequest.doc(sourceMap, XContentType.JSON);
        try {
            client.update(updateRequest, RequestOptions.DEFAULT);
            return Boolean.TRUE;
        } catch (Exception e) {
            log.error("esUtil#updateDoc#" + e.getMessage(), e);
            return Boolean.FALSE;
        }
    }

    /**
     * <p>
     * 批量增加join类型的父文档
     * </p>
     *
     * @param indexName  indexName
     * @param sourceMaps sourceMaps
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean batchAddParent(String indexName, String joinName, String parentName,
                                         List<? extends Map<String, Object>> sourceMaps) {
        if (CollUtil.isNotEmpty(sourceMaps)) {
            BulkRequest bulkRequest = new BulkRequest();
            int count = sourceMaps.size();
            for (Map<String, Object> v : sourceMaps) {
                Object id = v.get("id");
                if (checkParentDoc(indexName, Convert.toInt(id)) || ObjectUtil.isEmpty(id)) {
                    count--;
                    continue;
                }
                IndexRequest indexRequest = new IndexRequest(indexName);
                v.put(joinName, parentName);
                indexRequest.type("_doc")
                        .id(String.valueOf(v.remove("id")))
                        .source(v, XContentType.JSON);
                bulkRequest.add(indexRequest);
            }
            if (count == 0) {
                return Boolean.FALSE;
            }
            try {
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                return Boolean.TRUE;
            } catch (IOException e) {
                log.error("esUtil # batchAddParent #" + e.getMessage(), e);
                return Boolean.FALSE;
            }
        }
        return Boolean.FALSE;
    }


    /**
     * <p>
     * 批量更新join类型的父文档
     * </p>
     *
     * @param indexName  indexName
     * @param sourceMaps list
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean batchUpdateParent(String indexName, List<? extends Map<String, Object>> sourceMaps) {
        if (CollUtil.isNotEmpty(sourceMaps)) {
            BulkRequest bulkRequest = new BulkRequest();
            int count = sourceMaps.size();
            for (Map<String, Object> v : sourceMaps) {
                Object id = v.get("id");
                if (!checkParentDoc(indexName, Convert.toInt(id))) {
                    log.info(StrUtil.format("esUtil#batchUpdateParent#更新失败，id={}的文档不存在，无法更新", id));
                    count--;
                    continue;
                }
                UpdateRequest updateRequest = new UpdateRequest(indexName, "_doc",
                        String.valueOf(v.remove("id")));
                updateRequest.doc(v, XContentType.JSON);
                bulkRequest.add(updateRequest);
            }
            if (count == 0) {
                return Boolean.FALSE;
            }
            try {
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                return Boolean.TRUE;
            } catch (IOException e) {
                log.error("esUtil # batchUpdateParent #" + e.getMessage(), e);
                return Boolean.FALSE;
            }
        }
        return Boolean.FALSE;
    }

    /**
     * <p>
     * 批量删除文档
     * </p>
     *
     * @param indexName indexName
     * @param ids       ids
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean batchDelParent(String indexName, List<Integer> ids) {
        if (CollUtil.isNotEmpty(ids)) {
            BulkRequest bulkRequest = new BulkRequest();
            int count = ids.size();
            for (Integer id : ids) {
                if (!checkParentDoc(indexName, id)) {
                    log.info(id + "文档不存在，删除失败");
                    count--;
                    continue;
                }
                DeleteRequest deleteRequest = new DeleteRequest(indexName, "_doc", String.valueOf(id));
                List<Integer> childrenIds = getChildrenIds(indexName, "collection_content", id);
                if (childrenIds != null) {
                    childrenIds.forEach(i -> delChildDoc(indexName, i, id));
                }
                bulkRequest.add(deleteRequest);
            }
            if (count == 0) {
                return Boolean.FALSE;
            }
            try {
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                return Boolean.TRUE;
            } catch (IOException e) {
                log.error("esUtil # batchDel #" + e.getMessage(), e);
                return Boolean.FALSE;
            }
        }
        return Boolean.FALSE;
    }


    /**
     * <p>
     * 查看join类型的子文档是否存在
     * </p>
     *
     * @param indexName indexName
     * @param childId   childId
     * @param parentId  parentId
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean checkChildDoc(String indexName, Integer childId, Integer parentId) {
        GetRequest getRequest = new GetRequest(indexName, "_doc", String.valueOf(childId));
        getRequest.routing(String.valueOf(parentId));
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        try {
            return client.exists(getRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("esUtil # checkChildDoc #" + e.getMessage(), e);
            return Boolean.FALSE;
        }
    }

    /**
     * <p>
     * 查看join类型的父文档是否存在
     * </p>
     *
     * @param indexName indexName
     * @param id        id
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean checkParentDoc(String indexName, Integer id) {
        return checkDoc(indexName, id);
    }

    /**
     * <p>
     * 查看文档是否存在
     * </p>
     *
     * @param indexName indexName
     * @param id        id
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean checkDoc(String indexName, Integer id) {
        GetRequest getRequest = new GetRequest(indexName, "_doc", String.valueOf(id));
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        try {
            return client.exists(getRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("esUtil # checkDoc #" + e.getMessage(), e);
            return Boolean.FALSE;
        }
    }

    /**
     * <p>
     * 判断索引是否存在
     * </p>
     *
     * @param indexName indexName
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean checkIndex(String indexName) {
        try {
            return client.indices().exists(new GetIndexRequest().indices(indexName), RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("esUtil # checkIndex #" + e.getMessage(), e);
            return Boolean.FALSE;
        }
    }

    /**
     * <p>
     * 获取父文档中子文档的所有id
     * </p>
     *
     * @param indexName indexName
     * @return List
     * @author heshuyao
     * @since 2021/7/31
     */
    public static List<Integer> getChildrenIds(String indexName, String parentName, Integer parentId) {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        HasParentQueryBuilder parentQuery = JoinQueryBuilders.hasParentQuery(parentName,
                QueryBuilders.termQuery("_id", parentId), false);
        boolQueryBuilder.filter(parentQuery);
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("esUtil#getChildrenIds#" + e.getMessage(), e);
            return null;
        }
        return searchResponse != null ? Optional.of(Arrays.stream((searchResponse.getHits().getHits()))
                .map(i -> Integer.parseInt(i.getId()))
                .collect(Collectors.toList()))
                .orElse(new ArrayList<>()) : null;
    }

    /**
     * <p>
     * 获取企业文化的总记录数
     * </p>
     *
     * @param indexName indexName
     * @return Integer
     * @author heshuyao
     * @since 2021/8/2
     */
    public static Integer getCount(String indexName) {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("typeId", 2));
        SearchResponse search = null;
        try {
            search = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("esUtil#getCount#" + e.getMessage(), e);
        }
        return search != null ? Convert.toInt(search.getHits().getTotalHits()) : 0;
    }
}
