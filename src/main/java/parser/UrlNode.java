package parser;

import java.util.List;

public class UrlNode {
    private Integer level;
    private String url;
    private List<UrlNode> children;

    public UrlNode(Integer level, String element) {
        this.level = level;
        this.url = element;
    }

    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<UrlNode> getChildren() {
        return children;
    }

    public void setChildren(List<UrlNode> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < level * 3; i++) {
            sb.append(" ");
        }
        sb.append(url);
        return sb.toString();
    }
}
