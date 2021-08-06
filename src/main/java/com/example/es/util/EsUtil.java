package com.example.es.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import com.example.es.annotation.EsField;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Consumer;

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
                    filedValue = LocalDateTimeUtil.formatNormal(Convert.convert(LocalDateTime.class, filedValue));
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
     * 批量增加文档
     * </p>
     *
     * @param indexName  indexName
     * @param sourceMaps sourceMaps
     * @author heshuyao
     * @since 2021/8/1
     */
    public static void batchAddDoc(String indexName, List<? extends Map<String, Object>> sourceMaps, String routingName, String customKeyName) {
        if (CollUtil.isNotEmpty(sourceMaps)) {
            BulkRequest bulkRequest = new BulkRequest();
            sourceMaps.forEach(m -> {
                String id = Objects.toString(m.get("id"), null);
                String routing = Objects.toString(m.remove(routingName), null);
                String key = Objects.toString(m.remove(customKeyName), null);
                id = key != null ? key : id;
                IndexRequest indexRequest = new IndexRequest(indexName, "_doc", id);
                indexRequest.source(m, XContentType.JSON);
                if (StrUtil.isNotBlank(routing)) {
                    indexRequest.routing(routing);
                }
                bulkRequest.add(indexRequest);
            });
            dealBatchAsy(bulkRequest);
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
    public static Boolean delDoc(String indexName, String id, String routing) {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, "_doc", id);
        deleteRequest.routing(routing);
        try {
            return client.delete(deleteRequest, RequestOptions.DEFAULT).status() == RestStatus.OK ? Boolean.TRUE : Boolean.FALSE;
        } catch (Exception e) {
            log.error("esUtil # delDoc # {}", e.getMessage(), e);
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
    public static Boolean addDoc(String indexName, Map<String, Object> sourceMap, String routingName, String customKeyName) {
        String routing = Objects.toString(sourceMap.remove(routingName), null);
        String id = Objects.toString(sourceMap.get("id"), null);
        String key = Objects.toString(sourceMap.remove(customKeyName), null);
        id = key != null ? key : id;
        IndexRequest indexRequest = new IndexRequest(indexName, "_doc", id);
        indexRequest.source(sourceMap, XContentType.JSON).opType("create");
        if (StrUtil.isNotBlank(routing)) {
            indexRequest.routing(routing);
        }
        try {
            return client.index(indexRequest, RequestOptions.DEFAULT).status() == RestStatus.CREATED ? Boolean.TRUE : Boolean.FALSE;
        } catch (IOException e) {
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
    public static Boolean updateDoc(String indexName, Map<String, Object> sourceMap, String routingName, String customKeyName) {
        String id = Objects.toString(sourceMap.get("id"), null);
        String routing = Objects.toString(sourceMap.remove(routingName), null);
        String key = Objects.toString(sourceMap.remove(customKeyName), null);
        id = key != null ? key : id;
        UpdateRequest updateRequest = new UpdateRequest(indexName, "_doc", id);
        updateRequest.doc(sourceMap).opType();
        if (StrUtil.isNotBlank(routing)) {
            updateRequest.routing(routing);
        }
        try {
            return client.update(updateRequest, RequestOptions.DEFAULT).status() == RestStatus.OK ? Boolean.TRUE : Boolean.FALSE;
        } catch (IOException e) {
            log.error("esUtil # updateDoc # {}", e.getMessage(), e);
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
    public static void batchUpdateDoc(String indexName, List<? extends Map<String, Object>> sourceMaps, String routingName, String customKeyName) {
        if (CollUtil.isNotEmpty(sourceMaps)) {
            BulkRequest bulkRequest = new BulkRequest();
            sourceMaps.forEach(m -> {
                String routing = Objects.toString(m.remove(routingName), null);
                String id = Objects.toString(m.get("id"), null);
                String key = Objects.toString(m.remove(customKeyName), null);
                id = key != null ? key : id;
                UpdateRequest updateRequest = new UpdateRequest(indexName, "_doc", id);
                updateRequest.doc(m).opType();
                if (StrUtil.isNotBlank(routing)) {
                    updateRequest.routing(routing);
                }
                bulkRequest.add(updateRequest);
            });
            dealBatchAsy(bulkRequest);
        }
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
    public static void batchDelDoc(String indexName, List<String> ids) {
        if (CollUtil.isNotEmpty(ids)) {
            BulkRequest bulkRequest = new BulkRequest();
            ids.forEach(id -> {
                DeleteRequest deleteRequest = new DeleteRequest(indexName, "_doc", id);
                deleteRequest.opType();
                bulkRequest.add(deleteRequest);
            });
            dealBatchAsy(bulkRequest);
        }
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
    public static Boolean checkDoc(String indexName, String id, String routing) {
        GetRequest getRequest = new GetRequest(indexName);
        getRequest.fetchSourceContext(new FetchSourceContext(false)).storedFields("_none_");
        getRequest.type("_doc").id(id);
        if (StrUtil.isNotBlank(routing)) {
            getRequest.routing(routing);
        }
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
     * 异步处理批量操作
     * </p>
     *
     * @param bulkRequest bulkRequest
     * @author heshuyao
     * @since 2021/8/2
     */
    private static void dealBatchAsy(BulkRequest bulkRequest) {
        ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                log.info("批量操作ES数据成功");
            }

            @Override
            public void onFailure(Exception e) {
                log.error("esUtil # dealBatchAsy # {}", e.getMessage(), e);
            }
        };
        client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, listener);
    }
}
