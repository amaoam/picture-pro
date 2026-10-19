package com.zmt.yupicturebackendtry.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.zmt.yupicturebackendtry.model.dto.space.analyze.*;
import com.zmt.yupicturebackendtry.model.entity.Space;
import com.zmt.yupicturebackendtry.model.entity.User;
import com.zmt.yupicturebackendtry.model.vo.space.analyze.*;

import java.util.List;

/**
 * @author: ZMT
 * @date: 2026/3/26
 * @Description:
 */
public interface SpaceAnalyzeService extends IService<Space> {

    /**
     * 空间资源使用分析
     * @param spaceUsageAnalyzeRequest
     * @param loginUser
     * @return
     */
    SpaceUsageAnalyzeResponse getSpaceUsageAnalyze(SpaceUsageAnalyzeRequest spaceUsageAnalyzeRequest, User loginUser);

    /**
     * 空间权限校验
     * @param loginUser
     * @param space
     */
    void checkSpaceAuth(User loginUser, Space space);

    List<SpaceCategoryAnalyzeResponse> getSpaceCategoryAnalyze(SpaceCategoryAnalyzeRequest spaceCategoryAnalyzeRequest, User loginUser);

    List<SpaceTagAnalyzeResponse> getSpaceTagAnalyze(SpaceTagAnalyzeRequest spaceTagAnalyzeRequest, User loginUser);

    List<SpaceSizeAnalyzeResponse> getSpaceSizeAnalyze(SpaceSizeAnalyzeRequest spaceSizeAnalyzeRequest, User loginUser);

    List<SpaceUserAnalyzeResponse> getSpaceUserAnalyze(SpaceUserAnalyzeRequest spaceUserAnalyzeRequest, User loginUser);

    List<Space> getSpaceRankAnalyze(SpaceRankAnalyzeRequest spaceRankAnalyzeRequest, User loginUser);
}
