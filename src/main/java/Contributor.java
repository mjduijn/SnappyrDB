import lombok.Data;

@Data
public class Contributor {
    String login;
    int contributions;
    int id;
    String avatar_url;
}