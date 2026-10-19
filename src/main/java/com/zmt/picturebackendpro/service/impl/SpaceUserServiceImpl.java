
package com.zmt.picturebackendpro.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.ObjectUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.zmt.picturebackendpro.exception.BusinessException;
import com.zmt.picturebackendpro.exception.ErrorCode;
import com.zmt.picturebackendpro.exception.ThrowUtils;
import com.zmt.picturebackendpro.mapper.SpaceUserMapper;
import com.zmt.picturebackendpro.model.dto.spaceuser.SpaceUserAddRequest;
import com.zmt.picturebackendpro.model.dto.spaceuser.SpaceUserQueryRequest;
import com.zmt.picturebackendpro.model.entity.Space;
import com.zmt.picturebackendpro.model.entity.SpaceUser;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.enums.SpaceRoleEnum;
import com.zmt.picturebackendpro.model.vo.SpaceUserVO;
import com.zmt.picturebackendpro.model.vo.SpaceVO;
import com.zmt.picturebackendpro.model.vo.UserVO;
import com.zmt.picturebackendpro.service.SpaceService;
import com.zmt.picturebackendpro.service.SpaceUserService;
import com.zmt.picturebackendpro.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.stream.Collectors;

@Service

public class SpaceUserServiceImpl extends ServiceImpl<SpaceUserMapper, SpaceUser> implements SpaceUserService {
    @Resource
    private  UserService userService;
    @Lazy
    @Resource
    private  SpaceService spaceService;


    /**
     * 添加用户
     * @param spaceUserAddRequest
     * @return
     */
    @Override
    public long addSpaceUser(SpaceUserAddRequest spaceUserAddRequest) {
        // 参数校验
        ThrowUtils.throwIf(spaceUserAddRequest == null, ErrorCode.PARAMS_ERROR);
        SpaceUser spaceUser = new SpaceUser();
        BeanUtils.copyProperties(spaceUserAddRequest, spaceUser);
        validSpaceUser(spaceUser, true);
        // 数据库操作
        boolean result = this.save(spaceUser);
        ThrowUtils.throwIf(!result, ErrorCode.OPERATION_ERROR);
        return spaceUser.getId();
    }

    /**
     * 校验用户
     * @param spaceUser
     * @param add
     */
    @Override
    public void validSpaceUser(SpaceUser spaceUser, boolean add) {
        ThrowUtils.throwIf(spaceUser == null, ErrorCode.PARAMS_ERROR);
        // 创建时，空间 id 和用户 id 必填
        Long spaceId = spaceUser.getSpaceId();
        Long userId = spaceUser.getUserId();
        if (add) {
            ThrowUtils.throwIf(ObjectUtil.hasEmpty(spaceId, userId), ErrorCode.PARAMS_ERROR);
            User user = userService.getById(userId);
            ThrowUtils.throwIf(user == null, ErrorCode.NOT_FOUND_ERROR, "用户不存在");
            Space space = spaceService.getById(spaceId);
            ThrowUtils.throwIf(space == null, ErrorCode.NOT_FOUND_ERROR, "空间不存在");
        }
        // 校验空间角色
        String spaceRole = spaceUser.getSpaceRole();
        SpaceRoleEnum spaceRoleEnum = SpaceRoleEnum.getEnumByValue(spaceRole);
        if (spaceRole != null && spaceRoleEnum == null) {
            throw new BusinessException(ErrorCode.PARAMS_ERROR, "空间角色不存在");
        }
    }

    /**
     * 封装查询
     * @param spaceUserQueryRequest
     * @return
     */
    @Override
    public QueryWrapper<SpaceUser> getQueryWrapper(SpaceUserQueryRequest spaceUserQueryRequest) {
        QueryWrapper<SpaceUser> queryWrapper = new QueryWrapper<>();
        if (spaceUserQueryRequest == null) {
            return queryWrapper;
        }
        // 从对象中取值
        Long id = spaceUserQueryRequest.getId();
        Long spaceId = spaceUserQueryRequest.getSpaceId();
        Long userId = spaceUserQueryRequest.getUserId();
        String spaceRole = spaceUserQueryRequest.getSpaceRole();
        queryWrapper.eq(ObjUtil.isNotEmpty(id), "id", id);
        queryWrapper.eq(ObjUtil.isNotEmpty(spaceId), "spaceId", spaceId);
        queryWrapper.eq(ObjUtil.isNotEmpty(userId), "userId", userId);
        queryWrapper.eq(ObjUtil.isNotEmpty(spaceRole), "spaceRole", spaceRole);
        return queryWrapper;
    }

    /**
     * 空间成员封装类
     * @param spaceUser
     * @param request
     * @return
     */
    @Override
    public SpaceUserVO getSpaceUserVO(SpaceUser spaceUser, HttpServletRequest request) {
        // 对象转封装类
        SpaceUserVO spaceUserVO = SpaceUserVO.objToVo(spaceUser);
        // 关联查询用户信息
        Long userId = spaceUser.getUserId();
        if (userId != null && userId > 0) {
            User user = userService.getById(userId);
            UserVO userVO = userService.getUserVO(user);
            spaceUserVO.setUser(userVO);
        }
        // 关联查询空间信息
        Long spaceId = spaceUser.getSpaceId();
        if (spaceId != null && spaceId > 0) {
            Space space = spaceService.getById(spaceId);
            SpaceVO spaceVO = spaceService.getSpaceVO(space, request);
            spaceUserVO.setSpace(spaceVO);
        }
        return spaceUserVO;
    }


    /**
     * 封装类列表
     * @param spaceUserList
     * @return
     */
    @Override
    public List<SpaceUserVO> getSpaceUserVOList(List<SpaceUser> spaceUserList) {
        // 判断输入列表是否为空
        if (CollUtil.isEmpty(spaceUserList)) {
            return Collections.emptyList();
        }
        // 1. 收集需要关联查询的用户 ID 和空间 ID
        Set<Long> userIdSet = spaceUserList.stream().map(SpaceUser::getUserId).collect(Collectors.toSet());
        Set<Long> spaceIdSet = spaceUserList.stream().map(SpaceUser::getSpaceId).collect(Collectors.toSet());
        // 2. 批量查询用户和空间
        Map<Long, List<User>> userIdUserListMap = userService.listByIds(userIdSet).stream()
                .collect(Collectors.groupingBy(User::getId));
        Map<Long, List<Space>> spaceIdSpaceListMap = spaceService.listByIds(spaceIdSet).stream()
                .collect(Collectors.groupingBy(Space::getId));
        List<SpaceUserVO> spaceUserVOList = spaceUserList.stream().map(spaceUser -> {
            Long spaceId = spaceUser.getSpaceId();
            // --- 关键检查：如果 spaceId 在 Map 中找不到，说明该空间已被逻辑删除 ---
            if (!spaceIdSpaceListMap.containsKey(spaceId)) {
                return null; // 标记为 null
            }
            SpaceUserVO spaceUserVO = SpaceUserVO.objToVo(spaceUser);
            // 填充空间信息
            Space space = spaceIdSpaceListMap.get(spaceId).get(0);
            spaceUserVO.setSpace(SpaceVO.objToVo(space));
            // 填充用户信息
            User user = userIdUserListMap.getOrDefault(spaceUser.getUserId(), Collections.emptyList())
                    .stream().findFirst().orElse(null);
            spaceUserVO.setUser(userService.getUserVO(user));
            return spaceUserVO;
        }).filter(Objects::nonNull).toList();
        return spaceUserVOList;
    }







}