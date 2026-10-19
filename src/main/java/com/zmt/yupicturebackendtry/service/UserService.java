package com.zmt.yupicturebackendtry.service;

import com.zmt.yupicturebackendtry.model.entity.User;
import com.baomidou.mybatisplus.extension.service.IService;
import com.zmt.yupicturebackendtry.model.vo.LoginUserVO;
import com.zmt.yupicturebackendtry.model.vo.UserVO;

import javax.servlet.http.HttpServletRequest;

/**
* @author ZMT
* @description 针对表【user(用户)】的数据库操作Service
* @createDate 2026-03-21 20:05:56
*/
public interface UserService extends IService<User> {

    /**
     *  用户注册
     * @param userAccount
     * @param userPassword
     * @param checkPassword
     * @return
     */
    long userRegister(String userAccount,String userPassword,String checkPassword);

    /**
     *  获取加密之后的密码
     * @param userPassword
     * @return
     */
    String getEncryptPassword(String userPassword);

    /**
     * 用户登录
     *
     * @param userAccount  用户账户
     * @param userPassword 用户密码
     * @param request httpRequest 请求方便设置 cookie 
     * @return 脱敏后的用户信息
     */
    LoginUserVO userLogin(String userAccount, String userPassword, HttpServletRequest request);

    /**
     * 获取脱敏后的登录用户信息
     *
     * @param user 用户
     * @return 脱敏后的用户信息
     */
    LoginUserVO getLoginUserVO(User user);

    /**
     * 获得脱敏后的用户信息
     *
     * @param user
     * @return
     */
    UserVO getUserVO(User user);

    /**
     * 获取当前登录用户
     *
     * @param request
     * @return
     */
    User getLoginUser(HttpServletRequest request);
    
    

    /**
     * 用户注销
     *
     * @param request request
     * @return  注销结果
     */
    boolean userLogout(HttpServletRequest request);

    /**
     * 是否为管理员  
     *
     * @param user
     * @return
     */
    boolean isAdmin(User user);

    /**
     * 用户兑换会员（会员码兑换）
     */ 
    boolean exchangeVip(User user, String vipCode);
}
