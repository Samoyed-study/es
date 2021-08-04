package com.example.es.mapper;

import com.example.es.entity.User;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

/**
 * <p>
 *
 * </p>
 *
 * @author heshuyao
 * @since 2021/7/23 - 13:28
 */
public interface UserMapper extends ElasticsearchRepository<User, String> {
}
