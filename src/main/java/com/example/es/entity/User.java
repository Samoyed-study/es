package com.example.es.entity;

import com.example.es.annotation.EsField;
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
public class User {

    private String id;

    private String name;

    @EsField(isExist = false)
    private String sex;
}
