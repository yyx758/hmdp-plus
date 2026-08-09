package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.impl.BlogServiceImpl;
import com.hmdp.utils.UserHolder;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlogManagementServiceTest {

    private BlogServiceImpl blogService;
    private BlogMapper blogMapper;
    private StringRedisTemplate redisTemplate;
    private ZSetOperations<String, String> zSetOperations;
    private IFollowService followService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        blogService = new BlogServiceImpl();
        blogMapper = Mockito.mock(BlogMapper.class);
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        zSetOperations = Mockito.mock(ZSetOperations.class);
        followService = Mockito.mock(IFollowService.class);

        Mockito.when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        ReflectionTestUtils.setField(blogService, "baseMapper", blogMapper);
        ReflectionTestUtils.setField(blogService, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(blogService, "followService", followService);

        UserDTO currentUser = new UserDTO();
        currentUser.setId(7L);
        UserHolder.saveUser(currentUser);
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void authorUpdatesOnlyEditableFields() {
        Blog stored = new Blog()
                .setId(12L)
                .setUserId(7L)
                .setLiked(8)
                .setComments(3);
        Blog request = new Blog()
                .setUserId(99L)
                .setTitle("new title")
                .setContent("new content")
                .setImages("/imgs/new.png")
                .setShopId(5L)
                .setLiked(999)
                .setComments(999);
        Mockito.when(blogMapper.selectById(12L)).thenReturn(stored);
        Mockito.when(blogMapper.updateById(Mockito.any(Blog.class))).thenReturn(1);

        Result result = blogService.updateBlog(12L, request);

        assertTrue(result.getSuccess());
        ArgumentCaptor<Blog> captor = ArgumentCaptor.forClass(Blog.class);
        Mockito.verify(blogMapper).updateById(captor.capture());
        Blog update = captor.getValue();
        assertEquals(12L, update.getId());
        assertEquals("new title", update.getTitle());
        assertEquals("new content", update.getContent());
        assertEquals("/imgs/new.png", update.getImages());
        assertEquals(5L, update.getShopId());
        assertNull(update.getUserId());
        assertNull(update.getLiked());
        assertNull(update.getComments());
    }

    @Test
    void nonAuthorCannotUpdate() {
        Mockito.when(blogMapper.selectById(12L))
                .thenReturn(new Blog().setId(12L).setUserId(8L));

        Result result = blogService.updateBlog(12L, new Blog().setTitle("blocked"));

        assertFalse(result.getSuccess());
        Mockito.verify(blogMapper, Mockito.never()).updateById(Mockito.any(Blog.class));
    }

    @Test
    void authorDeletesBlogAndRedisReferences() {
        Mockito.when(blogMapper.selectById(12L))
                .thenReturn(new Blog().setId(12L).setUserId(7L));
        Mockito.when(blogMapper.deleteById(12L)).thenReturn(1);
        Mockito.when(followService.list(Mockito.any(QueryWrapper.class)))
                .thenReturn(Arrays.asList(
                        new Follow().setUserId(21L),
                        new Follow().setUserId(22L)));

        Result result = blogService.deleteBlog(12L);

        assertTrue(result.getSuccess());
        Mockito.verify(redisTemplate).delete(BLOG_LIKED_KEY + 12L);
        Mockito.verify(zSetOperations).remove(FEED_KEY + 21L, "12");
        Mockito.verify(zSetOperations).remove(FEED_KEY + 22L, "12");
    }

    @Test
    void databaseDeleteFailureDoesNotCleanRedis() {
        Mockito.when(blogMapper.selectById(12L))
                .thenReturn(new Blog().setId(12L).setUserId(7L));
        Mockito.when(blogMapper.deleteById(12L)).thenReturn(0);

        Result result = blogService.deleteBlog(12L);

        assertFalse(result.getSuccess());
        Mockito.verifyNoInteractions(redisTemplate);
    }

    @Test
    void missingBlogCannotBeDeleted() {
        Mockito.when(blogMapper.selectById(12L)).thenReturn(null);

        Result result = blogService.deleteBlog(12L);

        assertFalse(result.getSuccess());
        Mockito.verify(blogMapper, Mockito.never()).deleteById(Mockito.anyLong());
    }
}
