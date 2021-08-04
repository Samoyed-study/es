package com.example.es.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.LocalDateTime;

/**
 * <p>
 *
 * </p>
 *
 * @author heshuyao
 * @since 2021/7/24 - 18:39
 */
@Data
@Accessors(chain = true)
@ApiModel(value = "文章")
@Document(indexName = "companyCollection", type="familyArticle")
public class FamilyArticle {

    /**
     * 主键id
     */
    @Id
    @ApiModelProperty(value = "主键id")
    private Integer id;

    /**
     * 模块id（类型）
     */
    @Field(type = FieldType.Keyword)
    @ApiModelProperty(value = "模块id（类型）")
    private Integer moduleId;

    /**
     * 是否展示
     */
    @Field
    @ApiModelProperty(value = "是否展示")
    private Boolean isShow;

    /**
     * 名称
     */
    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    @ApiModelProperty(value = "名称")
    private String title;

    /**
     * 作者名称
     */
    @Field(type = FieldType.Keyword)
    @ApiModelProperty(value = "作者名称")
    private String authorName;

    /**
     * 发布时间
     */
    @Field(type = FieldType.Date, format= DateFormat.custom,pattern ="yyyy-MM-dd HH:mm:ss:SSS")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern ="yyyy-MM-dd HH:mm:ss:SSS",timezone="GMT+8")
    @ApiModelProperty(value = "发布时间")
    private LocalDateTime publishTime;

    /**
     * 发布时间
     */
    @Field(type = FieldType.Date, format=DateFormat.custom,pattern ="yyyy-MM-dd HH:mm:ss:SSS")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern ="yyyy-MM-dd HH:mm:ss:SSS",timezone="GMT+8")
    @ApiModelProperty(value = "收藏时间")
    private LocalDateTime collectTime;

    /**
     * 浏览次数
     */
    @Field(index = false)
    @ApiModelProperty(value = "浏览次数")
    private Integer viewed;
}
