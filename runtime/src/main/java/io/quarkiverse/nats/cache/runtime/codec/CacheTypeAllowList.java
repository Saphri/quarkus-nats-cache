package io.quarkiverse.nats.cache.runtime.codec;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator.Validity;

/**
 * Allow-list of the classes the codecs may instantiate from a stored {@code @class} type id.
 *
 * <p>Stored keys and values are read back from a shared NATS KV bucket, so the type ids embedded in them are
 * untrusted input: anyone with write access to the bucket could otherwise name an arbitrary class on the
 * classpath and have Jackson instantiate it (the classic polymorphic-deserialization gadget attack). Only
 * these classes are accepted:
 * <ul>
 *   <li>JDK value types in the packages {@code java.lang}, {@code java.util}, {@code java.time} and
 *       {@code java.math} (exact packages, not their sub-packages: e.g. {@code java.util.logging} is denied);</li>
 *   <li>classes in the application's own packages, registered at startup by the deployment module;</li>
 *   <li>classes under the package prefixes configured in {@code quarkus.nats-cache.allowed-packages}.</li>
 * </ul>
 *
 * <p>The decision is taken on the class <em>name</em>, before Jackson loads the class, so a denied class is
 * never initialized.
 *
 * <p>Instances are immutable apart from the warn-once bookkeeping, and nothing is shared between instances: the
 * recorder builds one per application from the build-time package list, so two applications in the same JVM
 * (test suites, dev-mode restarts) never see each other's packages.
 */
public final class CacheTypeAllowList {

    private static final Logger LOGGER = Logger.getLogger(CacheTypeAllowList.class);

    private static final Set<String> JDK_PACKAGES = Set.of("java.lang", "java.util", "java.time", "java.math");

    /** Packages whose classes (but not sub-packages) are allowed: the application's own packages. */
    private final Set<String> exactPackages;

    /** Package prefixes whose classes and sub-packages are allowed: {@code quarkus.nats-cache.allowed-packages}. */
    private final Set<String> prefixes;

    /** Denied class names already logged, so a misconfiguration warns once per class instead of once per read. */
    private final Set<String> reported = ConcurrentHashMap.newKeySet();

    private final PolymorphicTypeValidator validator = new Validator();

    /**
     * @param exactPackages packages whose classes are allowed, sub-packages excluded (e.g. {@code com.acme.orders})
     * @param packagePrefixes packages whose classes and all sub-packages are allowed (e.g. {@code com.acme}); a
     *        trailing dot is ignored, blank entries are skipped
     */
    public CacheTypeAllowList(Collection<String> exactPackages, Collection<String> packagePrefixes) {
        this.exactPackages = Set.copyOf(exactPackages);
        Set<String> normalized = new HashSet<>();
        for (String prefix : packagePrefixes) {
            String trimmed = prefix.trim();
            if (trimmed.endsWith(".")) {
                trimmed = trimmed.substring(0, trimmed.length() - 1);
            }
            if (!trimmed.isEmpty()) {
                normalized.add(trimmed);
            }
        }
        this.prefixes = Set.copyOf(normalized);
    }

    PolymorphicTypeValidator validator() {
        return validator;
    }

    boolean isAllowed(String className) {
        int lastDot = className.lastIndexOf('.');
        if (lastDot < 0) {
            return false; // default package: never produced by the codec
        }
        String pkg = className.substring(0, lastDot);
        if (JDK_PACKAGES.contains(pkg) || exactPackages.contains(pkg)) {
            return true;
        }
        for (String prefix : prefixes) {
            if (pkg.equals(prefix) || pkg.startsWith(prefix + ".")) {
                return true;
            }
        }
        return false;
    }

    private Validity check(String className) {
        if (isAllowed(className)) {
            return Validity.ALLOWED;
        }
        if (reported.add(className)) {
            LOGGER.warnf("NATS KV cache refused to deserialize class '%s': it is not in an allowed package. "
                    + "If this class is a legitimate cache key or value type, add its package to "
                    + "quarkus.nats-cache.allowed-packages", className);
        }
        return Validity.DENIED;
    }

    private final class Validator extends PolymorphicTypeValidator.Base {

        private static final long serialVersionUID = 1L;

        @Override
        public Validity validateSubClassName(MapperConfig<?> config, JavaType baseType, String subClassName) {
            if (subClassName.startsWith("[")) {
                return Validity.INDETERMINATE; // array: decided on the resolved element type in validateSubType
            }
            return check(subClassName);
        }

        @Override
        public Validity validateSubType(MapperConfig<?> config, JavaType baseType, JavaType subType) {
            JavaType type = subType;
            while (type.isArrayType()) {
                type = type.getContentType();
            }
            if (type.isPrimitive()) {
                return Validity.ALLOWED;
            }
            return check(type.getRawClass().getName());
        }
    }
}
