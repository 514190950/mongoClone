package com.tincery.mongoclone;


import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author gxz gongxuanzhang@foxmail.com
 **/
public class MongoGetter {

    final private int cloneCount;

    final private MongoTemplate oldMongoTemplate;

    final private MongoTemplate newMongoTemplate;

    private Map<String, List<Document>> clonedData;

    public MongoGetter(MongoTemplate oldMongoTemplate, MongoTemplate newMongoTemplate, int cloneCount) {
        this.oldMongoTemplate = oldMongoTemplate;
        this.newMongoTemplate = newMongoTemplate;
        this.cloneCount = cloneCount;
    }


    public Set<String> getCollectionNames() {
        return this.oldMongoTemplate.getCollectionNames();
    }

    public void scanOldMongo(Set<String> collectionNames, int cloneCount) {
        this.clonedData = new ConcurrentHashMap<>(collectionNames.size() * 4 / 3 + 1);
        Query query = new Query();
        if(cloneCount != -1){
            query.limit(cloneCount);
        }
        getCollectionNames().parallelStream()
                .forEach(collectionName -> {
                    List<Document> documentList = this.oldMongoTemplate.find(query, Document.class, collectionName);
                    this.clonedData.put(collectionName, documentList);
                });
    }


    public String writeNewMongo() {
        long startTime = Instant.now().toEpochMilli();
        this.clonedData.forEach((collectionName, data) -> newMongoTemplate.insert(data, collectionName));
        long endTime = Instant.now().toEpochMilli();
        String result = "共克隆" + clonedData.size() + "个集合，用时:" + (endTime - startTime) + "毫秒";
        this.clonedData = null;
        return result;
    }

    public void mongoClone(){
        this.scanOldMongo(getCollectionNames(),this.cloneCount);
        writeNewMongo();
    }
}
