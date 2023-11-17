package jsonComp;

import com.alibaba.fastjson.JSON;

public class JsonComp {
    public static void main(String[] args) {
        Student student = new Student("1", "zhangsan", "kb23", "dehua");
        String jsonString = JSON.toJSONString(student);
        System.out.println(jsonString);
    }
}
