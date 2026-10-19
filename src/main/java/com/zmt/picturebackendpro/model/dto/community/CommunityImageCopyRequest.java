package com.zmt.picturebackendpro.model.dto.community;

import lombok.Data;

import java.io.Serializable;

@Data
public class CommunityImageCopyRequest implements Serializable {
    private Long pictureId;
    private static final long serialVersionUID = 1L;
}
