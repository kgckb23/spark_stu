package jsonComp;

public class Student {
    private String stuId;
    private String stuName;
    private String classes;
    private String teacher;

    public Student() {
    }

    public Student(String stuId, String stuName, String classes, String teacher) {
        this.stuId = stuId;
        this.stuName = stuName;
        this.classes = classes;
        this.teacher = teacher;
    }

    /**
     * 获取
     * @return stuId
     */
    public String getStuId() {
        return stuId;
    }

    /**
     * 设置
     * @param stuId
     */
    public void setStuId(String stuId) {
        this.stuId = stuId;
    }

    /**
     * 获取
     * @return stuName
     */
    public String getStuName() {
        return stuName;
    }

    /**
     * 设置
     * @param stuName
     */
    public void setStuName(String stuName) {
        this.stuName = stuName;
    }

    /**
     * 获取
     * @return classes
     */
    public String getClasses() {
        return classes;
    }

    /**
     * 设置
     * @param classes
     */
    public void setClasses(String classes) {
        this.classes = classes;
    }

    /**
     * 获取
     * @return teacher
     */
    public String getTeacher() {
        return teacher;
    }

    /**
     * 设置
     * @param teacher
     */
    public void setTeacher(String teacher) {
        this.teacher = teacher;
    }

    public String toString() {
        return "Student{stuId = " + stuId + ", stuName = " + stuName + ", classes = " + classes + ", teacher = " + teacher + "}";
    }
}
