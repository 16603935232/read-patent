package com.patent.cn.test;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: shangml
 * @Date: 2023/06/02/16:43
 * @Description:
 */

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import com.patent.cn.result.Result;
import com.patent.cn.utils.ReadJsonUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.nio.file.Files;
import java.util.*;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.excludeId;

/**
 * 【专利】控制器层
 *
 * @author mmj
 * @date 2022-09-19
 */
@Slf4j
@Api(tags = {"【专利】模块Controller"})
@RestController
@RequestMapping("/patent")
public class PatentController {
    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @ApiOperation(value = "读取专利文件入库")
    @PostMapping()
    public Result<Boolean> readPatent(String filePath) {
        long statTime = System.currentTimeMillis();
        String[] filePaths = filePath.split(StrUtil.COMMA);
        for (String path : filePaths) {
            try {
                read(path);
            } catch (Exception e) {
                e.printStackTrace();
                return Result.fail(e.getMessage());
            }
        }
        long endTime = System.currentTimeMillis();
        log.info("读取全部文件完成，用时:{} ms", endTime - statTime);
        return Result.ok(Boolean.TRUE);
    }

    public void read(String filePath) throws Exception {
        if (StrUtil.isBlank(filePath)) {
            throw new Exception("文件夹路径不能为空");
        }
        File dir = new File(filePath);
        if (!dir.isDirectory()) {
            throw new Exception("非文件夹路径！");
        }
        List<File> allFileList = new LinkedList<>();
        getAllFile(dir, allFileList);
        if (allFileList != null && allFileList.size() > 0) {
            MongoCollection<Document> dbCollection;
            for (File item : allFileList) {
                long startTime = System.currentTimeMillis();
                log.info("读取{}路径下:{}文件开始", item.getParent(), item.getName());
                List<String> list = ReadJsonUtil.resultList(Files.newInputStream(item.toPath()));
                if (CollUtil.isNotEmpty(list)) {
                    String fileName = item.getName();
                    if (fileName.contains("history")) {
                        dbCollection = mongoTemplate.getCollection("history");
                    } else if (fileName.contains("legal")) {
                        dbCollection = mongoTemplate.getCollection("legal");
                    } else if (fileName.contains("patent")) {
                        dbCollection = mongoTemplate.getCollection("patent");
                    } else {
                        log.error("文件名：" + item.getName() + "，文件格式不正确");
                        continue;
                    }
                    List<WriteModel<Document>> bulkWrites = new LinkedList<>();
                    List<Document> documents = new LinkedList<>();
                    list.forEach(l -> documents.add(Document.parse(l)));
                    for (Document doc : documents) {
                        UpdateOneModel<Document> updateModel = new UpdateOneModel<>(
                                Filters.eq("_id", doc.get("_id")),
                                new Document("$set", doc),
                                new UpdateOptions().upsert(true)
                        );
                        bulkWrites.add(updateModel);
                    }
                    dbCollection.bulkWrite(bulkWrites);
                }
                long endTime = System.currentTimeMillis();
                log.info("读取{}路径下:{}文件完成，用时:{} ms", item.getParent(), item.getName(), endTime - startTime);
            }
        }
    }

    @ApiOperation(value = "更新企业和专利数据")
    @PostMapping("/update")
    public Result<Boolean> update() {
        long statTime = System.currentTimeMillis();
        MongoCollection<Document> collection = mongoTemplate.getCollection("patent");
        // 更新企业信息数据
        updateEnterprise(collection);
        // 更新企业与专利关联信息
        updateEnterprisePatent(collection);
        long endTime = System.currentTimeMillis();
        log.info("更新企业和专利数据完成，用时:{} ms", endTime - statTime);
        return Result.ok(Boolean.TRUE);
    }

    public void updateEnterprise(MongoCollection<Document> collection) {
        List<Bson> pipeline = Arrays.asList(
                unwind("$applicants"),
                unwind("$applicants.name.original"),
                group(
                        new Document("name", "$applicants.name.original")
                                .append("id", "$applicants.id")
                                .append("type", "$applicants.type")
                ),
                project(Projections.fields(
                        excludeId(),
                        computed("enterprise_name", "$_id.name"),
                        computed("enterprise_id", "$_id.id"),
                        computed("type", "$_id.type")
                )),
                out("enterprise")
        );
        collection.aggregate(pipeline).allowDiskUse(true).toCollection();
    }

    private void updateEnterprisePatent(MongoCollection<Document> collection) {
        List<Bson> pipeline = Arrays.asList(
                unwind("$applicants"),
                unwind("$applicants.name.original"),
                group(
                        new Document("parent_id", "$_id")
                                .append("enterprise_id", "$applicants.id")
                ),
                project(Projections.fields(
                        excludeId(),
                        computed("parent_id", "$_id.parent_id"),
                        computed("enterprise_id", "$_id.enterprise_id")
                )),
                out("enterprise_patent")
        );
        collection.aggregate(pipeline).allowDiskUse(true).toCollection();
    }

    public static void getAllFile(File fileInput, List<File> allFileList) {
        // 获取文件列表
        File[] fileList = fileInput.listFiles();
        assert fileList != null;
        for (File file : fileList) {
            if (file.isDirectory()) {
                // 递归处理文件夹
                // 如果不想统计子文件夹则可以将下一行注释掉
                getAllFile(file, allFileList);
            } else {
                // 如果是文件则将其加入到文件数组中
                allFileList.add(file);
            }
        }
    }

    @ApiOperation(value = "mongoDbToES")
    @PostMapping("/mongoDbToES")
    public Result<Boolean> mongoDbToES(String collectionName) {
        // 每页的文档数量
        MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
        // 游标遍历MongoDB集合
        try (MongoCursor<Document> cursor = collection.find().iterator()) {
            BulkRequest bulkRequest = new BulkRequest();
            // 每批处理的文档数量
            int batchSize = 1000;
            int count = 0;
            while (cursor.hasNext()) {
                Document document = cursor.next();
                // 使用文档ID作为Elasticsearch索引中文档的ID
                String id = document.get("_id").toString();
                // 删除MongoDB文档ID字段，因为它不是Elasticsearch索引所需要的数据
                document.remove("_id");
                // 将MongoDB文档转换为JSON字符串
                String json = document.toJson();
                // 创建Elasticsearch索引请求
                IndexRequest indexRequest = new IndexRequest(collectionName)
                        .id(id)
                        .source(json, XContentType.JSON);
                bulkRequest.add(indexRequest);
                count++;
                if (count % batchSize == 0) {
                    // 将索引写入批量请求
                    restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    bulkRequest = new BulkRequest();
                    log.info("已处理{}条文档。", count);
                }
            }
            // 处理剩余的文档
            if (bulkRequest.numberOfActions() > 0) {
                // 将索引写入批量请求
                restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            }
            log.info("处理完成，共处理{}条文档。", count);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Result.ok(Boolean.TRUE);
    }

    @ApiOperation(value = "添加ES索引的字段（在专利索引，增加history、legal对象字段）")
    @PostMapping("/insertIndexField")
    public Result<Boolean> insertIndexField(String collectionName, String indexName) {
        // 每页的文档数量
        MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
        // 游标遍历MongoDB集合
        try (MongoCursor<Document> cursor = collection.find().iterator()) {
            BulkRequest bulkRequest = new BulkRequest();
            // 每批处理的文档数量
            int batchSize = 1000;
            int count = 0;
            while (cursor.hasNext()) {
                Document document = cursor.next();
                // 使用文档ID作为Elasticsearch索引中文档的ID
                String id = document.get("_id").toString();
                // 删除MongoDB文档ID字段，因为它不是Elasticsearch索引所需要的数据
                document.remove("_id");
                // 将MongoDB文档转换为JSON字符串
                Map<String, Object> map = new HashMap(16);
                Map<String, Object> fieldMap = new HashMap(1);
                map.putAll(document);
                fieldMap.put(collectionName, map);
                // 创建Elasticsearch索引请求
                UpdateRequest updateRequest = new UpdateRequest(indexName, id)
                        .doc(fieldMap, XContentType.JSON);
                bulkRequest.add(updateRequest);
                count++;
                if (count % batchSize == 0) {
                    // 将索引写入批量请求
                    restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    bulkRequest = new BulkRequest();
                    log.info("已处理{}条文档。", count);
                }
            }
            // 处理剩余的文档
            if (bulkRequest.numberOfActions() > 0) {
                // 将索引写入批量请求
                restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            }
            log.info("处理完成，共处理{}条文档。", count);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Result.ok(Boolean.TRUE);
    }

}
