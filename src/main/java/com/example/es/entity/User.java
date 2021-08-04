package com.example.es.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * <p>
 *
 * </p>
 *
 * @author heshuyao
 * @since 2021/7/23 - 10:25
 */
@Data
@Accessors(chain = true)
@Document(indexName = "users", type="user")
public class User {

    @Id
    private String id;
    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String name;
    @Field(type = FieldType.Keyword)
    private String sex;
    @Field(type = FieldType.Keyword)
    private String tel;
}
