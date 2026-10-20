# yu-picture-backend 社区功能：首批代码

## 本批次实现范围

1. 新增 11 个社区相关实体与对应 MyBatis-Plus Mapper，对应你已经执行的 `community_feature.sql`。
2. 实现社区图片上传：
   - 仅保存社区：使用 `community/用户ID/` 对象路径，不写入 `picture` 表。
   - 保存到个人/团队空间：先复用现有 `PictureService.uploadPicture`，再复制 COS 对象到社区独立路径。
   - 选择现有图片：验证图片审核状态和个人/团队空间权限，再复制 COS 对象，避免公开帖子直接复用私有图片 URL。
3. 实现发帖（分享帖/求助帖）、帖子图片关联、标签关联、公开帖子分页/详情、分类和标签列表/创建。
4. 实现社区图片与帖子审核队列、审核操作，确保待审核内容不会进入公开列表。
5. 团队发布身份使用现有 `Space` / `SpaceUser`，后端只接受 `admin`/`editor` 角色进行团队身份发帖与向团队空间保存图片。

## 未包含的功能

本批次尚未实现点赞、评论、个人经验发放/等级升级、帖子编辑/删除、评论管理及完整内容举报功能。这些是接下来独立实现的模块；点赞和经验值不应直接塞入发帖逻辑。

## 接口

所有接口基于现有登录态；写接口通过 `UserService.getLoginUser(request)` 获取用户，客户端不能传 `userId`。

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/community/image/upload` | multipart 上传；参数 `file`、`saveToSpace`、可选 `spaceId` |
| POST | `/community/image/copy` | 复制已有图库图片，JSON `{ "pictureId": 123 }` |
| GET | `/community/image/admin/page` | 管理员图片审核队列，默认待审核；可传 `current`、`pageSize`、`status` |
| POST | `/community/image/{id}/review` | 管理员审核图片：1 通过、2 拒绝 |
| GET | `/community/category/list` | 获取启用分类 |
| GET | `/community/tag/list` | 获取标签，可传 `keyword` |
| POST | `/community/tag` | 创建或取回同名标签，JSON `{ "tagName": "夜景" }` |
| POST | `/community/post` | 创建帖子；初始状态为待审核 |
| POST | `/community/post/page` | 公开帖子分页（只返回已发布） |
| GET | `/community/post/{id}` | 获取公开帖子详情 |
| POST | `/community/post/admin/page` | 管理员帖子审核队列 |
| POST | `/community/post/{id}/review` | 管理员审核帖子：1 发布、2 拒绝、3 下架 |

### 发帖 JSON 示例

```json
{
  "postType": 1,
  "publishType": 1,
  "publishSpaceId": null,
  "title": "夜景摄影作品分享",
  "content": "分享这次拍摄的参数和处理思路。",
  "categoryId": 1,
  "imageIds": [10001],
  "tagIds": [20001]
}
```

- `postType`: `1` 分享帖，`2` 求助帖。
- `publishType`: `1` 个人身份，`2` 团队身份。
- `publishType=2` 时必须传有效的团队 `publishSpaceId`；个人身份不能传 `publishSpaceId`。
- 分享帖至少关联一张社区图片；求助帖可无图。
- `imageIds` 是 `community_image.id`，不是现有 `picture.id`。现有 `picture.id` 先调用 `/community/image/copy`。
- `tagIds` 是 `community_tag.id`；先调用标签列表或创建接口获取。
- 发帖后为待审核状态。新上传的社区图片也默认待审核，需先审核图片，再审核帖子。

## 本地验收建议

1. 未登录调用发帖/图片上传应被现有登录态逻辑拒绝。
2. 上传文件且 `saveToSpace=false`，确认 `community_image` 新增记录，而 `picture` 表不新增记录。
3. 上传文件且 `saveToSpace=true`，确认目标空间新增 `picture` 记录和社区副本记录。
4. 普通用户不能向不属于自己的个人空间上传；团队 `viewer` 不能向团队空间上传或使用团队身份发帖。
5. 选择 `reviewStatus != 1` 的既有 `picture` 应被拒绝。
6. 分享帖不传图应报参数错误；求助帖可以不传图。
7. 新发帖应在管理员审核通过前不出现在公开分页。
8. 社区图片待审核时，帖子不能被审核发布；图片拒绝后不能公开发布引用该图片的帖子。
9. 检查 COS 对象复制出的主图和缩略图都位于 `community/` 路径，原图仍在原路径。
