package parser;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

public class HtmlParser {
    private static String baseUrl;
    private static Integer deepLevel;

    public static void main(String[] args) {
        HtmlParser p = new HtmlParser();
        baseUrl = "https://yandex.ru/support";
        deepLevel = 2;
        List<UrlNode> nodes = p.parse(baseUrl);
        p.print(nodes.get(0));
    }

    public List<UrlNode> parse(String url) {
        UrlNode n = new UrlNode(0, url);
        List<UrlNode> urlNodes = Collections.synchronizedList(new ArrayList<>());
        urlNodes.add(n);
        Set<String> s = Collections.synchronizedSet(new TreeSet<>());
        s.add(n.getUrl());
        CustomRecursiveTask t = new CustomRecursiveTask(n, s, urlNodes);
        ForkJoinPool f = ForkJoinPool.commonPool();
        f.invoke(t);
        return urlNodes;
    }

    private void print(UrlNode node) {
        System.out.println(node);
        if (node.getChildren() != null && !node.getChildren().isEmpty())
            node.getChildren().forEach(this::print);
    }

    private static class CustomRecursiveTask extends RecursiveTask<List<UrlNode>> {
        private final Set<String> existingUrls;
        private final UrlNode urlNode;
        private final List<UrlNode> localUrls;

        public CustomRecursiveTask(UrlNode urlNode, Set<String> existingUrls, List<UrlNode> localUrls) {
            this.localUrls = localUrls;
            this.urlNode = urlNode;
            this.existingUrls = existingUrls;
        }

        @Override
        protected List<UrlNode> compute() {
            Map<String, Integer> internalLinks = getInternalLinks(urlNode);
            List<UrlNode> newNodes = internalLinks.entrySet().stream().filter(entry -> !existingUrls.contains(entry.getKey()))
                    .map(entry -> new UrlNode(entry.getValue(), entry.getKey()))
                    .collect(Collectors.toList());
            urlNode.setChildren(newNodes);
            localUrls.addAll(newNodes);
//            System.out.println(Thread.currentThread().getName());
            Set<String> existing = new TreeSet<>(existingUrls);
            existing.addAll(newNodes.stream()
                    .map(UrlNode::getUrl)
                    .collect(Collectors.toList()));
            if (deepLevel - 1 > urlNode.getLevel()) {
                List<CustomRecursiveTask> customRecursiveTasks = newNodes.stream()
                        .map(node -> new CustomRecursiveTask(node, existing, localUrls))
                        .collect(Collectors.toList());
                ForkJoinTask.invokeAll(customRecursiveTasks)
                        .stream()
                        .map(ForkJoinTask::join)
                        .collect(Collectors.toList());
            }
            return localUrls;
        }

        private Map<String, Integer> getInternalLinks(UrlNode node) {
            try {
                URL url = new URL(baseUrl);
                Document document = Jsoup.connect(node.getUrl()).get();
                Map<String, Integer> map = document.getElementsByTag("a").stream()
                        .map(element -> element.attr("href"))
                        .filter(s -> s.startsWith(url.getPath()))
                        .collect(Collectors.toMap(val -> baseUrl.replace(url.getPath(), "") + val, l -> node.getLevel() + 1, (k, v) -> v));
                return map;
            } catch (
                    IOException e) {
                e.printStackTrace();
            }
            return Collections.emptyMap();
        }
    }
}