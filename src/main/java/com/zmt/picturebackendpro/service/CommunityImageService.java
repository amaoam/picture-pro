package com.zmt.picturebackendpro.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.zmt.picturebackendpro.model.entity.User;
import com.zmt.picturebackendpro.model.vo.community.CommunityImageVO;
import org.springframework.web.multipart.MultipartFile;

public interface CommunityImageService {
    /** 上传社区图片。saveToSpace=true 时同时写入现有个人/团队图库。 */
    CommunityImageVO uploadImage(MultipartFile file, Long spaceId, boolean saveToSpace, User loginUser);

    /** 从用户有权访问且已审核通过的图库图片创建社区公开副本。 */
    CommunityImageVO copyExistingPicture(Long pictureId, User loginUser);

    /** 管理员分页查询图片审核队列。 */
    Page<CommunityImageVO> pageReviewImages(long current, long pageSize, Integer status, User loginUser);

    /** 管理员审核社区图片：1-通过，2-拒绝。 */
    void reviewImage(Long imageId, Integer status, String reviewMessage, User loginUser);
}
