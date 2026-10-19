package com.zmt.picturebackendpro.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zmt.picturebackendpro.model.dto.space.SpaceAddRequest;
import com.zmt.picturebackendpro.model.dto.space.SpaceQueryRequest;
import com.zmt.picturebackendpro.model.entity.Space;
import com.baomidou.mybatisplus.extension.service.IService;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.SpaceVO;

import javax.servlet.http.HttpServletRequest;

/**
* @author ZMT
* @description 针对表【space(空间)】的数据库操作Service
* @createDate 2026-03-24 20:13:10
*/
public interface SpaceService extends IService<Space> {
    /**
     * 校验空间数据
     * @param space
     * @param add
     */
    void validSpace(Space space, boolean add);

    /**
     * 根据空间级别自动填充数据
     * @param space
     */
    void fillSpaceBySpaceLevel(Space space);

    /**
     * 创建空间
     *
     * @param spaceAddRequest
     * @param loginUser
     * @return
     */
    long addSpace(SpaceAddRequest spaceAddRequest, User loginUser);

    /**
     * 获取空间包装类（单条）
     *
     * @param space
     * @param request
     * @return
     */
    SpaceVO getSpaceVO(Space space, HttpServletRequest request);

    /**
     * 获取查询对象
     *
     * @param spaceQueryRequest
     * @return
     */
    QueryWrapper<Space> getQueryWrapper(SpaceQueryRequest spaceQueryRequest);

    /**
     * 获取空间包装类（分页）
     *
     * @param spacePage
     * @param request
     * @return
     */
    Page<SpaceVO> getSpaceVOPage(Page<Space> spacePage, HttpServletRequest request);
    
    /**
     * 校验空间权限
     *
     * @param loginUser
     * @param space
     */
    void checkSpaceAuth(User loginUser, Space space);
}
