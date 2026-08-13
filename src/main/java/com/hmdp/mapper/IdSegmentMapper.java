package com.hmdp.mapper;

import com.hmdp.entity.IdSegment;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface IdSegmentMapper {

    @Select("SELECT biz_tag AS bizTag, max_id AS maxId, step "
            + "FROM tb_id_segment WHERE biz_tag = #{bizTag} FOR UPDATE")
    IdSegment lockByBizTag(@Param("bizTag") String bizTag);

    @Update("UPDATE tb_id_segment SET max_id = max_id + #{step}, "
            + "version = version + 1 WHERE biz_tag = #{bizTag}")
    int advance(@Param("bizTag") String bizTag, @Param("step") int step);
}
