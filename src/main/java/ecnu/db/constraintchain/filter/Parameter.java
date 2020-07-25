package ecnu.db.constraintchain.filter;

/**
 * @author alan
 * 代表需要实例化的参数
 */
public class Parameter {
    private Integer id;
    private String data;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Parameter(Integer id, String data) {
        this.id = id;
        this.data = data;
    }

    @Override
    public String toString() {
        return "Parameter{" +
                "id=" + id +
                ", data='" + data + '\'' +
                '}';
    }
}
