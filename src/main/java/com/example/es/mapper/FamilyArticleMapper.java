package com.example.es.mapper;

import com.example.es.entity.FamilyArticle;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

/**
 * <p>
 *
 * </p>
 *
 * @author heshuyao
 * @since 2021/7/24 - 19:19
 */
public interface FamilyArticleMapper extends ElasticsearchRepository<FamilyArticle, Integer> {

}
