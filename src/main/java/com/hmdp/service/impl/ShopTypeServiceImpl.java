package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_TYPE_LIST_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries(SHOP_TYPE_LIST_KEY);

        if(!entries.isEmpty()){
            return Result.ok(entries.values().stream()
                    .map(obj -> JSONUtil.toBean(obj.toString(), ShopType.class))
                    .sorted(Comparator.comparing(ShopType::getSort))
                    .collect(Collectors.toList()));
        }

        List<ShopType> typeList = query().orderByAsc("sort").list();

        if (typeList == null){
            return Result.fail("Shop type list is null");
        }

        Map<String, String> hashMap = new HashMap<>();
        for (ShopType type : typeList) {
            hashMap.put(type.getId().toString(), JSONUtil.toJsonStr(type));
        }
        stringRedisTemplate.opsForHash().putAll(SHOP_TYPE_LIST_KEY, hashMap);
        // 设置过期时间（可选）
//        stringRedisTemplate.expire(SHOP_TYPE_LIST_KEY, 30, TimeUnit.MINUTES);

        return Result.ok(typeList);
    }
}
