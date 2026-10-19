package com.zmt.yupicturebackendtry.model.dto.community;

import lombok.Data;

import java.io.Serializable;

@Data
public class CommunityTagAddRequest implements Serializable {
    private String tagName;
    private static final long serialVersionUID = 1L;
}
