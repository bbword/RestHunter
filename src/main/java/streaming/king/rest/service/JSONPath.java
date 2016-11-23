package streaming.king.rest.service;

import com.jayway.jsonpath.internal.JsonContext;

/**
 * 5/25/16 WilliamZhu(allwefantasy@gmail.com)
 */
public class JSONPath {
    public static <T> T read(String json, String jsonPath) {
        return new JsonContext().parse(json).read(jsonPath);
    }
}
