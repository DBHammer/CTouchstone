package ecnu.db.constraintchain.filter;

import com.fasterxml.jackson.annotation.ObjectIdGenerator;
import com.fasterxml.jackson.annotation.ObjectIdResolver;

import java.util.HashMap;
import java.util.Map;

/**
 * @author alan
 */
public class ParameterResolver implements ObjectIdResolver {
    public static final Map<ObjectIdGenerator.IdKey, Object> ITEMS = new HashMap<>();

    @Override
    public void bindItem(ObjectIdGenerator.IdKey id, Object pojo) {
        if (ITEMS.containsKey(id)) {
            throw new IllegalStateException("Already had POJO for id (" + id.key.getClass().getName() + ") [" + id + "]");
        }
        ITEMS.put(id, pojo);
    }

    @Override
    public Object resolveId(ObjectIdGenerator.IdKey id) {
        Object object = ITEMS.get(id);
        return object == null ? getById(id) : object;
    }

    protected Object getById(ObjectIdGenerator.IdKey id) {
        Object object;
        try {
            object = id.scope.getConstructor().newInstance();
            id.scope.getMethod("setId", Integer.class).invoke(object, id.key);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        ITEMS.put(id, object);
        return object;
    }

    @Override
    public ObjectIdResolver newForDeserialization(Object context) {
        return new ParameterResolver();
    }

    @Override
    public boolean canUseFor(ObjectIdResolver resolverType) {
        return resolverType.getClass() == getClass();
    }
}
