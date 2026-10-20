package com.zmt.picturebackendpro.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zmt.picturebackendpro.model.dto.community.CommunityPostAddRequest;
import com.zmt.picturebackendpro.model.dto.community.CommunityPostQueryRequest;
import com.zmt.picturebackendpro.model.entity.CommunityCategory;
import com.zmt.picturebackendpro.model.entity.CommunityTag;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.community.CommunityPostVO;


import java.util.List;

public interface CommunityPostService {
    Long createPost(CommunityPostAddRequest request, User loginUser);

    Page<CommunityPostVO> pagePublicPosts(CommunityPostQueryRequest request);

    Page<CommunityPostVO> pageAdminPosts(CommunityPostQueryRequest request, User loginUser);

    CommunityPostVO getPublicPost(Long postId);

    void reviewPost(Long postId, Integer status, User loginUser);

    List<CommunityCategory> listCategories();

    List<CommunityTag> listTags(String keyword);

    CommunityTag createOrGetTag(String tagName, User loginUser);
}
