package com.example.es.service.serviceImpl;

import org.springframework.stereotype.Service;

/**
 * <p>
 *
 * </p>
 *
 * @author heshuyao
 * @since 2021/8/4 - 9:32
 */
@Service
public class EsServiceImpl {
    // @PostConstruct
    // public void init() {
    //     if (EsUtil.getCountByTypeId(EsIndexConstant.MY_COLLECTION_INDEX) == 0) {
    //         List<FamilyArticle> articles = Optional.ofNullable(familyArticleService.lambdaQuery()
    //                 .select(FamilyArticle::getId, FamilyArticle::getTitle, FamilyArticle::getContent,
    //                         FamilyArticle::getIsShow, FamilyArticle::getAuthorId, FamilyArticle::getAuthorName,
    //                         FamilyArticle::getPublishTime, FamilyArticle::getCreateTime, FamilyArticle::getCreateUser,
    //                         FamilyArticle::getCreateUserName)
    //                 .list()).orElse(new ArrayList<>());
    //         List<Map<String, Object>> maps = articles.stream()
    //                 .map(r -> EsUtil.toEsMap(r, map -> map.put("typeId", 2)))
    //                 .collect(Collectors.toList());
    //         EsUtil.batchAddParent(EsIndexConstant.MY_COLLECTION_INDEX, "data_type", "collection_content",
    //                 maps);
    //         List<FamilyArticleCollectionRecord> records = Optional.ofNullable(familyArticleCollectionRecordService
    //                 .lambdaQuery().select(FamilyArticleCollectionRecord::getId, FamilyArticleCollectionRecord::getArticleId,
    //                         FamilyArticleCollectionRecord::getCreateUser)
    //                 .list()).orElse(new ArrayList<>());
    //         EsUtil.batchAddChild(EsIndexConstant.MY_COLLECTION_INDEX, "data_type", "collection_user",
    //                 "articleId", EsUtil.toEsMaps(records));
    //     }
    // }
}
