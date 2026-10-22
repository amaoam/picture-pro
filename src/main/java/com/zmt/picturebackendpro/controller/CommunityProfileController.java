package com.zmt.picturebackendpro.controller;

import com.zmt.picturebackendpro.common.BaseResponse;
import com.zmt.picturebackendpro.common.ResultUtils;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.community.UserCommunityProfileVO;
import com.zmt.picturebackendpro.service.CommunityExperienceService;
import com.zmt.picturebackendpro.service.UserService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/community/profile")
public class CommunityProfileController {
    @Resource private CommunityExperienceService experienceService;
    @Resource private UserService userService;
    
    //查询个人社区资料
    @GetMapping("/me")
    public BaseResponse<UserCommunityProfileVO> myProfile(HttpServletRequest request) {
        User user = userService.getLoginUser(request);
        return ResultUtils.success(experienceService.getProfile(user.getId()));
    }
    
    //查询他人社区资料
    @GetMapping("/{userId}")
    public BaseResponse<UserCommunityProfileVO> profile(@PathVariable("userId") Long userId) {
        return ResultUtils.success(experienceService.getProfile(userId));
    }
}
