# yu-picture-backend 社区功能增量代码（互动 + 等级成长）

目标项目：公开仓库的 `yu-picture-backend` 普通分层后端

本代码包是在上一批“社区图片资源、发帖、分页、审核”代码基础上继续扩展的完整增量目录。将 `src/main/java/com/yupi/yupicturebackend/` 合并到本地同名目录；不要单独复制某几个类而遗漏实体主键映射修复。

## 本批次实现

1. 社区帖子点赞 / 取消点赞 / 当前用户点赞状态。
2. 公共图库图片点赞 / 取消点赞 / 当前用户点赞状态。
3. 创建评论、一级回复、评论分页、删除自己的评论；顶层评论存在回复时需先删除回复。
4. 经验与等级服务：帖子首次审核发布奖励、评论奖励、帖子/图片被点赞奖励、评论删除后撤销原评论经验。
5. 发帖和评论经验按用户、业务日期分别限额；获赞经验不设每日上限。
6. 经验事件以 `user_exp_record.eventKey` 唯一约束幂等，同一帖子/评论/点赞事件不会重复发放经验。
7. 在帖子审核从非发布状态转为已发布时接入发帖经验服务。
8. 修正上一批 `UserCommunityProfile` 实体的主键映射：该表以 `userId` 为主键，必须显式标记 `@TableId(value = "userId", type = IdType.INPUT)`。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/community/post/{postId}/like` | 点赞已发布帖子 |
| DELETE | `/community/post/{postId}/like` | 取消帖子点赞 |
| GET | `/community/post/{postId}/like/status` | 查询当前用户点赞状态和点赞数 |
| POST | `/community/picture/{pictureId}/like` | 点赞已审核通过的公共图库图片 |
| DELETE | `/community/picture/{pictureId}/like` | 取消公共图片点赞 |
| GET | `/community/picture/{pictureId}/like/status` | 查询图片点赞状态和点赞数 |
| POST | `/community/post/{postId}/comments` | 创建顶层评论或一级回复，JSON 中传 `content` 和可选 `parentId` |
| POST | `/community/post/{postId}/comments/page` | 评论分页，JSON 中可传 `current`、`pageSize` |
| DELETE | `/community/comment/{commentId}` | 删除自己的评论；管理员也可删除 |
| GET | `/community/profile/me` | 获取自己的社区经验、等级和统计 |
| GET | `/community/profile/{userId}` | 查询用户公开社区档案 |

所有写操作需要现有登录态。帖子点赞状态接口需要登录，因为响应包括 `likedByCurrentUser`。评论分页、公开档案查询无需登录。

## 经验规则默认值

可在 `application.yml` 中增加以下配置，不配置时会采用默认值：

```yaml
community:
  experience:
    post-reward: 10
    comment-reward: 2
    received-like-reward: 1
    daily-post-cap: 30
    daily-comment-cap: 20
    level-thresholds: 0,100,300,600,1000,1500,2200,3000,4000,5500
    zone-id: Asia/Shanghai
```

- 发帖经验在帖子首次审核发布时发放；待审核和被拒绝的帖子不发经验。
- 发帖、评论每日上限限制的是该类行为当天可以获得的经验总量，不限制行为次数；临近上限时只发剩余额度。
- 获赞经验没有每日上限；帖子和公共图库图片的有效点赞都会奖励内容所有者。
- 自己不能给自己的帖子或公共图库图片点赞。
- 对同一个 `postId + likerId` 或 `pictureId + likerId`，取消后再点赞不会重复拿获赞经验，但 `receivedLikeCount` 仍跟随当前有效点赞关系增减。
- 评论删除会撤销该评论当时实际获得的经验；被每日上限截断为 0 经验的评论不会产生负经验。
- 删除评论不返还当天评论经验额度，避免“评论、删除、再评论”循环刷取限额。
- 等级阈值表示达到该累计经验时进入对应等级。例如 `0,100,300` 表示等级 1 起始经验 0，100 经验升到等级 2，300 经验升到等级 3。

这些值是可调整的初始配置，不代表产品最终数值。等级最高配置为阈值列表长度；超过最高阈值后不再自动增加配置等级。

## 与现有 SQL 的关系

本批次不新增数据库表，不要求重新执行此前的 `community_feature.sql`。它使用已有以下表：

- `community_post`
- `community_comment`
- `community_post_like`
- `picture_like`
- `user_community_profile`
- `user_exp_record`
- 原项目 `picture`、`user`

请确认这几张社区表均已存在，且 `user_exp_record.eventKey`、`community_post_like(postId,userId)`、`picture_like(pictureId,userId)` 的唯一索引已创建。

## 重要实现细节

- 点赞关系表的唯一索引是并发重复点赞的最终防线。只有成功插入/删除关系记录时，才调整点赞统计或接收点赞统计。
- `community_post.likeCount` / `commentCount` 使用数据库原子加减，避免普通“先查再写”造成的并发丢计数。
- 用户社区档案以 `userId` 为主键，经验变更时通过 `SELECT ... FOR UPDATE` 锁定该用户的档案行，再计算每日限额并写入流水。
- 经验流水会为哪怕奖励金额为 0 的已处理业务事件留下一条记录，用于保证这个事件不会因重复请求在之后重新发奖。
- 帖子获赞经验的 eventKey 示例：`POST_RECEIVED_LIKE:{postId}:{likerId}`；图片获赞的 eventKey 示例：`PICTURE_RECEIVED_LIKE:{pictureId}:{likerId}`。
- COS 和数据库事务的原子性不属于本批次范围；本批次主要涉及 MySQL 内同事务写入。

