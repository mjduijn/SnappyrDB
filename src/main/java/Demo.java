import retrofit2.Retrofit;
import retrofit2.adapter.rxjava.RxJavaCallAdapterFactory;
import retrofit2.converter.gson.GsonConverterFactory;
import retrofit2.http.GET;
import retrofit2.http.Path;
import rx.Observable;
import snappyrdb.Context;
import snappyrdb.SnappyrDB;
import snappyrdb.extenders.PutIn;
import snappyrdb.operators.AssignKey;

import java.util.List;

public class Demo {
    public static final String API_URL = "https://api.github.com";

    public interface GitHubService {
        @GET("/repos/{owner}/{repo}/contributors")
        Observable<List<Contributor>> contributors(@Path("owner") String owner, @Path("repo") String repo);
    }

    public static GitHubService createGitHubService() {
        // Create a very simple REST adapter which points the GitHub API.
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(API_URL)
                .addConverterFactory(GsonConverterFactory.create())
                .addCallAdapterFactory(RxJavaCallAdapterFactory.create())
                .build();

        // Create an instance of our GitHub API interface.
        return retrofit.create(GitHubService.class);
    }


    public static void main(String... args) {
        System.out.println("Running demo");
        SnappyrDB snappyrdb = new SnappyrDB(new Context());

        GitHubService github = createGitHubService();

        github.contributors("fptudelft", "IN4355-2015")
                .flatMap(contributors -> Observable.from(contributors))
                .doOnNext(contributor -> System.out.println(contributor))
                .filter(contributor -> !contributor.getLogin().toLowerCase().equals("mjduijn"))
                .lift(new AssignKey<>(contributor ->  contributor.getLogin()))
                .extend(new PutIn<>(snappyrdb))
                .subscribe(error -> System.out.println(error),
                        () -> System.out.println("Successfully inserted elements"));
    }
}
