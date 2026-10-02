package io.quarkiverse.nats.cache.runtime.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ValueCodecTest {

    public static final class Order {
        private String id;
        private List<Item> items;

        public Order() {
        }

        public Order(String id, List<Item> items) {
            this.id = id;
            this.items = items;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public List<Item> getItems() {
            return items;
        }

        public void setItems(List<Item> items) {
            this.items = items;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Order))
                return false;
            Order order = (Order) o;
            return java.util.Objects.equals(id, order.id) && java.util.Objects.equals(items, order.items);
        }

        @Override
        public int hashCode() {
            return 31 * java.util.Objects.hashCode(id) + java.util.Objects.hashCode(items);
        }
    }

    public static final class Item {
        private String sku;
        private int qty;

        public Item() {
        }

        public Item(String sku, int qty) {
            this.sku = sku;
            this.qty = qty;
        }

        public String getSku() {
            return sku;
        }

        public void setSku(String sku) {
            this.sku = sku;
        }

        public int getQty() {
            return qty;
        }

        public void setQty(int qty) {
            this.qty = qty;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Item))
                return false;
            Item item = (Item) o;
            return qty == item.qty && java.util.Objects.equals(sku, item.sku);
        }

        @Override
        public int hashCode() {
            return 31 * java.util.Objects.hashCode(sku) + qty;
        }
    }

    // --- value round-trips ----------------------------------------------------

    @Test
    void pojoValuesPreserveExactRuntimeClass() {
        Order order = new Order("o-1", List.of(new Item("sku-1", 2), new Item("sku-2", 5)));
        ValueCodec.Envelope env = ValueCodec.decode(ValueCodec.encode(order, null));
        assertThat(env.data()).isInstanceOf(Order.class).isEqualTo(order);
        // nested elements keep their concrete types
        List<?> items = ((Order) env.data()).getItems();
        assertThat(items.get(0)).isInstanceOf(Item.class);
    }

    @Test
    void scalarAndCollectionValuesRoundTrip() {
        assertThat(ValueCodec.decode(ValueCodec.encode("hello", null)).data()).isEqualTo("hello");
        assertThat(ValueCodec.decode(ValueCodec.encode(42, null)).data()).isEqualTo(42).isInstanceOf(Integer.class);
        assertThat(ValueCodec.decode(ValueCodec.encode(42L, null)).data()).isInstanceOf(Long.class);
        assertThat(ValueCodec.decode(ValueCodec.encode(1.5d, null)).data()).isEqualTo(1.5d);
        assertThat(ValueCodec.decode(ValueCodec.encode(Boolean.FALSE, null)).data()).isEqualTo(Boolean.FALSE);

        List<String> list = List.of("a", "b");
        assertThat(ValueCodec.decode(ValueCodec.encode(list, null)).data()).isEqualTo(list);

        Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put("n", 1);
        map.put("s", "x");
        assertThat(ValueCodec.decode(ValueCodec.encode(map, null)).data()).isEqualTo(map);
    }

    @Test
    void valueResemblingEnvelopeRoundTripsUnambiguously() {
        // A user value whose JSON shape mimics the envelope ({@code data}/{@code exp} members) must not be
        // confused with an actual envelope: it is embedded as the typed encoding of the {@code data} member.
        Map<String, Object> value = new java.util.LinkedHashMap<>();
        value.put("data", "x");
        value.put("exp", 123L);
        ValueCodec.Envelope env = ValueCodec.decode(ValueCodec.encode(value, null));
        assertThat(env.data()).isEqualTo(value);
    }

    // --- null sentinel ---------------------------------------------------------

    @Test
    void cachedNullIsStoredAsDataNullEnvelope() {
        byte[] payload = ValueCodec.encode(null, null);
        String json = new String(payload, java.nio.charset.StandardCharsets.UTF_8);
        assertThat(json).contains("\"data\":null");

        ValueCodec.Envelope env = ValueCodec.decode(payload);
        assertThat(env.data()).isNull();
        assertThat(env.expiresAtEpochMillis()).isNull();
        assertThat(env.isExpired(System.currentTimeMillis())).isFalse();
    }

    @Test
    void cachedNullWithExpirationIsStillDistinguishableFromMiss() {
        long now = System.currentTimeMillis();
        ValueCodec.Envelope live = ValueCodec.decode(ValueCodec.encode(null, now + 60_000));
        assertThat(live.data()).isNull();
        assertThat(live.isExpired(now)).isFalse();

        ValueCodec.Envelope stale = ValueCodec.decode(ValueCodec.encode(null, now - 1));
        assertThat(stale.data()).isNull();
        assertThat(stale.isExpired(now)).isTrue();
    }

    // --- logical TTL (exp) ------------------------------------------------------

    @Test
    void liveEnvelopeIsNotExpired() {
        long now = System.currentTimeMillis();
        ValueCodec.Envelope env = ValueCodec.decode(ValueCodec.encode("v", now + 60_000));
        assertThat(env.expiresAtEpochMillis()).isEqualTo(now + 60_000);
        assertThat(env.isExpired(now)).isFalse();
    }

    @Test
    void expiredEnvelopeIsDetected() {
        long now = System.currentTimeMillis();
        ValueCodec.Envelope env = ValueCodec.decode(ValueCodec.encode("v", now - 1));
        assertThat(env.isExpired(now)).isTrue();
    }

    @Test
    void envelopeWithoutExpNeverExpires() {
        long now = System.currentTimeMillis();
        ValueCodec.Envelope env = ValueCodec.decode(ValueCodec.encode("v", null));
        assertThat(env.expiresAtEpochMillis()).isNull();
        assertThat(env.isExpired(now)).isFalse();
        assertThat(env.isExpired(Long.MAX_VALUE)).isFalse();
    }

    // --- unknown / legacy payloads ---------------------------------------------

    @Test
    void plainScalarPayloadFailsCleanly() {
        byte[] payload = "\"just a string\"".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ValueCodec.decode(payload))
                .isInstanceOf(CacheValueException.class)
                .hasMessageContaining("envelope");
    }

    @Test
    void objectWithoutDataMemberFailsCleanly() {
        byte[] payload = "{\"foo\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ValueCodec.decode(payload))
                .isInstanceOf(CacheValueException.class)
                .hasMessageContaining("data");
    }

    @Test
    void nonJsonPayloadFailsCleanly() {
        assertThatThrownBy(() -> ValueCodec.decode(new byte[] { 1, 2, 3 }))
                .isInstanceOf(CacheValueException.class);
    }

    @Test
    void nullPayloadFailsCleanly() {
        assertThatThrownBy(() -> ValueCodec.decode(null))
                .isInstanceOf(CacheValueException.class);
    }

    @Test
    void nonNumericExpFailsCleanly() {
        byte[] payload = "{\"exp\":\"soon\",\"data\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ValueCodec.decode(payload))
                .isInstanceOf(CacheValueException.class)
                .hasMessageContaining("exp");
    }
}
