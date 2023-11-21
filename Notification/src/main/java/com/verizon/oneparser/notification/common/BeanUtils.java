package com.verizon.oneparser.notification.common;

import java.beans.PropertyDescriptor;
import java.util.HashSet;
import java.util.Set;

import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

/**
 * Utility class related to bean manipulation.
 *
 */
public class BeanUtils {

    /**
     * Copies non-null properties from one bean to another. This is particularly useful for the POST request as they
     * often support both creation and update.
     * <p>
     * Note: this is just one way to do it. If issues are encountered, it could also be implemented with Jackson
     * updating mechanism (ObjectMapper.readerForUpdating).
     *
     * @param source the source object.
     * @param target the target object.
     */
    public static void updateBean(Object source, Object target) {
        if (source == null) {
            throw new IllegalArgumentException("source class must not be null");
        }

        if (target == null) {
            throw new IllegalArgumentException("target class must not be null");
        }

        if (target instanceof HibernateProxy) {
            LazyInitializer lazyInitializer = ((HibernateProxy) target).getHibernateLazyInitializer();
            target = lazyInitializer.getImplementation();
        }

        // enforce using the same type for source and target classes, to prevent possible programming mistakes.
        if (!source.getClass().equals(target.getClass())) {
            throw new IllegalArgumentException("source and target class whould have the same type, source: "
                    + source.getClass() + ", target: " + target.getClass());
        }

        //log.trace("updating entity {}", target);
        org.springframework.beans.BeanUtils.copyProperties(source, target, getNullPropertyNames(source));
    }

    /**
     * Lists bean properties that have a null value. This is used by {@link #updateBean(Object, Object)} to smartly
     * update a bean by copying non-null values from another one.
     */
    private static String[] getNullPropertyNames(Object object) {
        final BeanWrapper src = new BeanWrapperImpl(object);
        PropertyDescriptor[] propertyDescriptors = src.getPropertyDescriptors();

        Set<String> emptyProperties = new HashSet<>();
        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            Object value = src.getPropertyValue(propertyDescriptor.getName());
            if (value == null) {
                emptyProperties.add(propertyDescriptor.getName());
            }
        }
        String[] result = new String[emptyProperties.size()];
        return emptyProperties.toArray(result);
    }
}
