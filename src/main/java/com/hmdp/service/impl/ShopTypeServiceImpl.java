package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_TYPE_KEY;

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
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        String key = CACHE_TYPE_KEY;
        // 1.redis中有缓存，直接返回
        String typeJson = stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(typeJson)){
            List<ShopType> shopTypeList = JSONUtil.toList(typeJson, ShopType.class);
            return Result.ok(shopTypeList);
        }
        // 2.没有，到数据库中查询
        List<ShopType> typeList = query().orderByAsc("sort").list();
        if(typeList.isEmpty()){
            // 3.空，返回错误
            return Result.fail("无店家类型");
        }
        // 4.将数据写入到redis中
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(typeList));
        // 5.返回
        return Result.ok(typeList);
    }
}
