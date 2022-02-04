package org.karakarua.domain;

/**
 * 定义一个可以被处理为流的POJO
 * Flink要求POJO必须具备
 * <ul>
 *     <li>公有且独立的类（public、没有非静态内部类）</li></li>
 *     <li>一个无参的构造器</li>
 *     <li>所有成员的get方法和set方法，且方法名遵循JavaBean命名规范，及getXxx形式</li>
 * </ul>
 * 参考 <a href="https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/learn-flink/datastream_api/#pojos">POJOs</a>
 */
public class UserInfo {
    String username;
    String password;

    public UserInfo() {
    }

    public UserInfo(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserInfo userInfo = (UserInfo) o;

        if (!username.equals(userInfo.username)) return false;
        return password.equals(userInfo.password);
    }

    @Override
    public int hashCode() {
        int result = username.hashCode();
        result = 31 * result + password.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
