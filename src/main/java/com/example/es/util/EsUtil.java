package com.example.es.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReflectUtil;
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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Component;

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
 * elasticSearch 工具类
 * </p>
 *
 * @author heshuyao
 * @since 2021/7/27 - 17:28
 */
@Slf4j
@Component
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
            log.error("esUtil # search #{}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * <p>
     * 将传入对象中要存入es的字段和属性进行转换
     * </p>
     *
     * @param source source
     * @param extra  extra  需要加入的额外字段
     * @return Map
     * @author heshuyao
     * @since 2021/7/30
     */
    public static Map<String, Object> toEsMap(Object source, Consumer<? super Map<String, Object>> extra) {
        Map<String, Object> esMap = new HashMap<>(11);
        Field[] fields = ReflectUtil.getFields(source.getClass());
        Arrays.stream(fields).forEach(field -> {
            field.setAccessible(true);
            EsField esField = field.getAnnotation(EsField.class);
            if (esField != null && !esField.isExist()) {
                return;
            }
            String filedName = esField != null ? esField.name() : field.getName();
            try {
                Object filedValue = field.get(source);
                if (filedValue != null && filedValue.getClass().equals(LocalDateTime.class)) {
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    filedValue = dtf.format(Convert.convert(LocalDateTime.class, filedValue));
                }
                esMap.put(filedName, filedValue);
            } catch (IllegalAccessException e) {
                log.error("esUtil # toEsMap #{}", e.getMessage(), e);
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
    public static List<Map<String, Object>> toEsMaps(List<?> list, Consumer<? super Map<String, Object>> extra) {
        if (CollUtil.isNotEmpty(list)) {
            return list.stream().map(a -> EsUtil.toEsMap(a, extra)).collect(Collectors.toList());
        }
        return null;
    }


    /**
     * <p>
     * 批量增加文档
     * </p>
     *
     * @param indexName  indexName
     * @param sourceMaps sourceMaps
     * @return Boolean
     * @author heshuyao
     * @since 2021/8/1
     */
    public static Boolean batchAddDoc(String indexName, List<? extends Map<String, Object>> sourceMaps, String routAddress) {
        if (CollUtil.isNotEmpty(sourceMaps)) {
            BulkRequest bulkRequest = new BulkRequest();
            int count = sourceMaps.size();
            for (Map<String, Object> m : sourceMaps) {
                Object id = m.get("id");
                if (checkDoc(indexName, Convert.toInt(id), String.valueOf(routAddress)) || ObjectUtil.isEmpty(id)) {
                    count--;
                    continue;
                }
                IndexRequest indexRequest = new IndexRequest(indexName, "_doc", String.valueOf(m.remove("id")));
                indexRequest.source(m);
                indexRequest.routing(routAddress);
                bulkRequest.add(indexRequest);
            }
            if (count == 0) {
                return Boolean.FALSE;
            }
            try {
                return client.bulk(bulkRequest, RequestOptions.DEFAULT).status() == RestStatus.OK ? Boolean.TRUE : Boolean.FALSE;
            } catch (IOException e) {
                log.error("esUtil # batchAddChild #{}", e.getMessage(), e);
                return Boolean.FALSE;
            }
        }
        return Boolean.FALSE;
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
    public static Boolean delDoc(String indexName, Integer id, String routAddress) {
        if (!checkDoc(indexName, id, routAddress)) {
            return Boolean.FALSE;
        }
        DeleteRequest deleteRequest = new DeleteRequest(indexName, "_doc", String.valueOf(id));
        deleteRequest.routing(routAddress);
        try {
            return client.delete(deleteRequest, RequestOptions.DEFAULT).status() == RestStatus.OK ? Boolean.TRUE : Boolean.FALSE;
        } catch (Exception e) {
            log.error("esUtil # delDoc #{}", e.getMessage(), e);
            return Boolean.FALSE;
        }
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
    public static Boolean addDoc(String indexName, Map<String, Object> sourceMap, String routAddress) {
        if (checkDoc(indexName, Convert.toInt(sourceMap.get("id")), routAddress)) {
            return Boolean.FALSE;
        }
        IndexRequest indexRequest = new IndexRequest(indexName, "_doc");
        indexRequest.id(String.valueOf(sourceMap.remove("id"))).source(sourceMap, XContentType.JSON);
        indexRequest.routing(routAddress);
        try {
            return client.index(indexRequest, RequestOptions.DEFAULT).status() == RestStatus.OK ? Boolean.TRUE : Boolean.FALSE;
        } catch (Exception e) {
            log.error("esUtil # addDoc #{}", e.getMessage(), e);
            return Boolean.FALSE;
        }
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
    public static Boolean updateDoc(String indexName, Map<String, Object> sourceMap, String routAddress) {
        if (!checkDoc(indexName, Convert.toInt(sourceMap.get("id")), routAddress)) {
            return Boolean.FALSE;
        }
        UpdateRequest updateRequest = new UpdateRequest(indexName, "_doc", String.valueOf(sourceMap.remove("id")));
        updateRequest.routing(routAddress);
        updateRequest.doc(sourceMap, XContentType.JSON);
        try {
            return client.update(updateRequest, RequestOptions.DEFAULT).status() == RestStatus.OK ? Boolean.TRUE : Boolean.FALSE;
        } catch (Exception e) {
            log.error("esUtil # updateDoc #{}", e.getMessage(), e);
            return Boolean.FALSE;
        }
    }

    /**
     * <p>
     * 批量更新文档
     * </p>
     *
     * @param indexName  indexName
     * @param sourceMaps list
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean batchUpdateDoc(String indexName, List<? extends Map<String, Object>> sourceMaps) {
        if (CollUtil.isNotEmpty(sourceMaps)) {
            BulkRequest bulkRequest = new BulkRequest();
            int count = sourceMaps.size();
            for (Map<String, Object> v : sourceMaps) {
                Object id = v.get("id");
                if (!checkDoc(indexName, Convert.toInt(id), null)) {
                    count--;
                    continue;
                }
                UpdateRequest updateRequest = new UpdateRequest(indexName, "_doc", String.valueOf(v.remove("id")));
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
                log.error("esUtil # batchUpdateParent # {}", e.getMessage(), e);
                return Boolean.FALSE;
            }
        }
        return Boolean.FALSE;
    }

    /**
     * <p>
     * 批量删除父文档
     * </p>
     *
     * @param indexName indexName
     * @param ids       ids
     * @return Boolean
     * @author heshuyao
     * @since 2021/7/29
     */
    public static Boolean batchDelDoc(String indexName, List<Integer> ids, String routAddress) {
        if (CollUtil.isNotEmpty(ids)) {
            BulkRequest bulkRequest = new BulkRequest();
            int count = ids.size();
            for (Integer id : ids) {
                if (!checkDoc(indexName, id, null)) {
                    count--;
                    continue;
                }
                DeleteRequest deleteRequest = new DeleteRequest(indexName, "_doc", String.valueOf(id));
                deleteRequest.routing(routAddress);
                bulkRequest.add(deleteRequest);
            }
            if (count == 0) {
                return Boolean.FALSE;
            }
            try {
                return client.bulk(bulkRequest, RequestOptions.DEFAULT).status() == RestStatus.OK ? Boolean.TRUE : Boolean.FALSE;
            } catch (IOException e) {
                log.error("esUtil# bulk # {}", e.getMessage(), e);
                return Boolean.FALSE;
            }
        }
        return Boolean.FALSE;
    }


    /**
     * <p>
     * 判断是否存在index或doc时调用此方法
     * </p>
     *
     * @param getRequest getRequest
     * @author heshuyao
     * @since 2021/8/4
     */
    private static void setNoFetchAndStore(GetRequest getRequest) {
        getRequest.fetchSourceContext(new FetchSourceContext(false)).storedFields("_none_");
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
    public static Boolean checkDoc(String indexName, Integer id, String routAddress) {
        GetRequest getRequest = new GetRequest(indexName, "_doc", String.valueOf(id));
        EsUtil.setNoFetchAndStore(getRequest);
        getRequest.routing(routAddress);
        try {
            return client.exists(getRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("esUtil # checkDoc # {}", e.getMessage(), e);
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
            log.error("esUtil # checkIndex # {}", e.getMessage(), e);
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
        HasParentQueryBuilder parentQuery = JoinQueryBuilders.hasParentQuery(parentName,
                QueryBuilders.termQuery("_id", parentId), false);
        searchSourceBuilder.query(parentQuery);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = EsUtil.search(searchRequest);
        return searchResponse != null ? Optional.of(Arrays.stream((searchResponse.getHits().getHits()))
                .map(i -> Integer.parseInt(i.getId()))
                .collect(Collectors.toList()))
                .orElse(new ArrayList<>()) : null;
    }

    /**
     * <p>
     * 根据类型获取记录数
     * </p>
     *
     * @param indexName indexName
     * @return Integer
     * @author heshuyao
     * @since 2021/8/2
     */
    public static Integer getCountByTypeId(String indexName, Integer typeId) {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("typeId", typeId));
        SearchResponse search = null;
        try {
            search = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("esUtil # getCountByTypeId # {}", e.getMessage(), e);
        }
        return search.status() == RestStatus.OK ? Convert.toInt(search.getHits().totalHits) : 0;
    }
}
