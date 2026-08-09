package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class ShopTypeServiceImpl
        extends ServiceImpl<ShopTypeMapper, ShopType>
        implements IShopTypeService {

    private static final String SHOP_TYPE_CACHE_KEY =
            "cache:shoptype:list";

    private static final long SHOP_TYPE_CACHE_TTL =
            30L;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ObjectMapper objectMapper;

    @Override
    public List<ShopType> queryTypeList() {

        // 1. 查询 Redis
        String json = stringRedisTemplate.opsForValue()
                .get(SHOP_TYPE_CACHE_KEY);

        // 2. 缓存命中
        if (StringUtils.hasText(json)) {
            try {
                log.debug("缓存命中");
                return objectMapper.readValue(
                        json,
                        new TypeReference<List<ShopType>>() {
                        }
                );
            } catch (JsonProcessingException e) {
                // 缓存内容损坏，删除缓存，继续查询数据库
                stringRedisTemplate.delete(SHOP_TYPE_CACHE_KEY);

                log.warn("店铺类型缓存解析失败，已删除缓存", e);
            }
        }

        // 3. 缓存未命中，查询数据库
        List<ShopType> typeList = lambdaQuery()
                .orderByAsc(ShopType::getSort)
                .list();

        // 4. 将数据库结果写入 Redis
        // 即使是空集合，也会序列化成 []
        try {
            String cacheValue =
                    objectMapper.writeValueAsString(typeList);

            stringRedisTemplate.opsForValue().set(
                    SHOP_TYPE_CACHE_KEY,
                    cacheValue,
                    SHOP_TYPE_CACHE_TTL,
                    TimeUnit.MINUTES
            );
        } catch (JsonProcessingException e) {
            // 缓存写入失败，不影响数据库查询结果返回
            log.warn("店铺类型序列化失败，跳过缓存写入", e);
        }

        // 5. 返回数据库查询结果
        return typeList;
    }
}