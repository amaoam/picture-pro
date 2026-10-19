package com.zmt.picturebackendpro.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.IService;
import com.zmt.picturebackendpro.model.dto.spaceuser.SpaceUserAddRequest;
import com.zmt.picturebackendpro.model.dto.spaceuser.SpaceUserQueryRequest;
import com.zmt.picturebackendpro.model.entity.SpaceUser;
import com.zmt.picturebackendpro.model.vo.SpaceUserVO;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

public interface SpaceUserService extends IService<SpaceUser> {

    long addSpaceUser(SpaceUserAddRequest spaceUserAddRequest);

    void validSpaceUser(SpaceUser spaceUser, boolean add);

    QueryWrapper<SpaceUser> getQueryWrapper(SpaceUserQueryRequest spaceUserQueryRequest);

    SpaceUserVO getSpaceUserVO(SpaceUser spaceUser, HttpServletRequest request);

    List<SpaceUserVO> getSpaceUserVOList(List<SpaceUser> spaceUserList);
}