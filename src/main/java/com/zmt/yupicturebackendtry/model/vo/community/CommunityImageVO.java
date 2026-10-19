package com.zmt.yupicturebackendtry.model.vo.community;

import com.zmt.yupicturebackendtry.model.entity.CommunityImage;
import lombok.Data;
import org.springframework.beans.BeanUtils;

import java.io.Serializable;

/**
 * 社区图片视图
 */
@Data
public class CommunityImageVO implements Serializable {
    private Long id;
    private Long ownerUserId;
    private Integer sourceType;
    private Long sourcePictureId;
    private Long savedPictureId;
    private String name;
    private String url;
    private String thumbnailUrl;
    private Long picSize;
    private Integer picWidth;
    private Integer picHeight;
    private String picFormat;
    private Integer reviewStatus;

    public static CommunityImageVO objToVo(CommunityImage image) {
        if (image == null) {
            return null;
        }
        CommunityImageVO vo = new CommunityImageVO();
        BeanUtils.copyProperties(image, vo);
        return vo;
    }

    private static final long serialVersionUID = 1L;
}
