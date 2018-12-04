package introdb.engine;

public class Config {
    private final int pageSize;
    private final int maxNrPages;

    private Config(int pageSize, int maxNrPages) {
        this.pageSize = pageSize;
        this.maxNrPages = maxNrPages;
    }

    public static Config of(int pageSize, int maxNrPages) {
        return new Config(pageSize, maxNrPages);
    }

    public int pageSize() {
        return pageSize;
    }

    public int MaxNrPages() {
        return maxNrPages;
    }
}
