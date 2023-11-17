package jsonComp;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class School {
    private String name;
    private RoomInfo roomInfo;
    private List<Teacher> teachers = new ArrayList<Teacher>();
    private List<Student> students = new ArrayList<Student>();

    public static void main(String[] args) {
        School school = new School();
        school.setName("南京中博");
        RoomInfo roomInfo = new RoomInfo(30, 2000, 98);
        school.setRoomInfo(roomInfo);

        Teacher gree = new Teacher("gree", "bigdata&java", 7);
        Teacher xingxing = new Teacher("xingxing", "bigdata&java", 3);
        school.teachers.add(gree);
        school.teachers.add(xingxing);

        Student student1 = new Student("1", "zhangsan", "kb01", "xingxing");
        Student student2 = new Student("2", "lisi", "kb02", "gree");
        Student student3 = new Student("3", "wangwu", "kb03", "gree");
        Student student4 = new Student("4", "zhaoliu", "kb04", "xingxing");
        school.students.add(student1);
        school.students.add(student2);
        school.students.add(student3);
        school.students.add(student4);

        String jsonString = JSON.toJSONString(school);
        System.out.println(jsonString);

    }

}
