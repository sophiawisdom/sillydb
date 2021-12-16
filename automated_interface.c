#include "db_interface.h"

#include <xmmintrin.h>

static void SHA1_SHAEXT_Transform(uint32_t state[5], const uint32_t *data)
{
    __m128i ABCD, ABCD_SAVE, E0, E0_SAVE, E1;
    __m128i MASK, MSG0, MSG1, MSG2, MSG3;

    // Load initial values
    ABCD = _mm_loadu_si128((__m128i*) state);
    E0 = _mm_set_epi32(state[4], 0, 0, 0);
    ABCD = _mm_shuffle_epi32(ABCD, 0x1B);
    MASK = _mm_set_epi64x(0x0001020304050607ULL, 0x08090a0b0c0d0e0fULL);

    // Save current hash
    ABCD_SAVE = ABCD;
    E0_SAVE = E0;

    // Rounds 0-3
    MSG0 = _mm_loadu_si128((__m128i*) data+0);
    MSG0 = _mm_shuffle_epi8(MSG0, MASK);
    E0 = _mm_add_epi32(E0, MSG0);
    E1 = ABCD;
    ABCD = _mm_sha1rnds4_epu32(ABCD, E0, 0);

    // Rounds 4-7
    MSG1 = _mm_loadu_si128((__m128i*) (data+4));
    MSG1 = _mm_shuffle_epi8(MSG1, MASK);
    E1 = _mm_sha1nexte_epu32(E1, MSG1);
    E0 = ABCD;
    ABCD = _mm_sha1rnds4_epu32(ABCD, E1, 0);
    MSG0 = _mm_sha1msg1_epu32(MSG0, MSG1);

    // Rounds 8-11
    MSG2 = _mm_loadu_si128((__m128i*) (data+8));
    MSG2 = _mm_shuffle_epi8(MSG2, MASK);
    E0 = _mm_sha1nexte_epu32(E0, MSG2);
    E1 = ABCD;
    ABCD = _mm_sha1rnds4_epu32(ABCD, E0, 0);
    MSG1 = _mm_sha1msg1_epu32(MSG1, MSG2);
    MSG0 = _mm_xor_si128(MSG0, MSG2);

    // Rounds 12-15
    MSG3 = _mm_loadu_si128((__m128i*) (data+12));
    MSG3 = _mm_shuffle_epi8(MSG3, MASK);
    E1 = _mm_sha1nexte_epu32(E1, MSG3);
    E0 = ABCD;
    MSG0 = _mm_sha1msg2_epu32(MSG0, MSG3);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E1, 0);
    MSG2 = _mm_sha1msg1_epu32(MSG2, MSG3);
    MSG1 = _mm_xor_si128(MSG1, MSG3);

    // Rounds 16-19
    E0 = _mm_sha1nexte_epu32(E0, MSG0);
    E1 = ABCD;
    MSG1 = _mm_sha1msg2_epu32(MSG1, MSG0);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E0, 0);
    MSG3 = _mm_sha1msg1_epu32(MSG3, MSG0);
    MSG2 = _mm_xor_si128(MSG2, MSG0);

    // Rounds 20-23
    E1 = _mm_sha1nexte_epu32(E1, MSG1);
    E0 = ABCD;
    MSG2 = _mm_sha1msg2_epu32(MSG2, MSG1);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E1, 1);
    MSG0 = _mm_sha1msg1_epu32(MSG0, MSG1);
    MSG3 = _mm_xor_si128(MSG3, MSG1);

    // Rounds 24-27
    E0 = _mm_sha1nexte_epu32(E0, MSG2);
    E1 = ABCD;
    MSG3 = _mm_sha1msg2_epu32(MSG3, MSG2);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E0, 1);
    MSG1 = _mm_sha1msg1_epu32(MSG1, MSG2);
    MSG0 = _mm_xor_si128(MSG0, MSG2);

    // Rounds 28-31
    E1 = _mm_sha1nexte_epu32(E1, MSG3);
    E0 = ABCD;
    MSG0 = _mm_sha1msg2_epu32(MSG0, MSG3);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E1, 1);
    MSG2 = _mm_sha1msg1_epu32(MSG2, MSG3);
    MSG1 = _mm_xor_si128(MSG1, MSG3);

    // Rounds 32-35
    E0 = _mm_sha1nexte_epu32(E0, MSG0);
    E1 = ABCD;
    MSG1 = _mm_sha1msg2_epu32(MSG1, MSG0);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E0, 1);
    MSG3 = _mm_sha1msg1_epu32(MSG3, MSG0);
    MSG2 = _mm_xor_si128(MSG2, MSG0);

    // Rounds 36-39
    E1 = _mm_sha1nexte_epu32(E1, MSG1);
    E0 = ABCD;
    MSG2 = _mm_sha1msg2_epu32(MSG2, MSG1);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E1, 1);
    MSG0 = _mm_sha1msg1_epu32(MSG0, MSG1);
    MSG3 = _mm_xor_si128(MSG3, MSG1);

    // Rounds 40-43
    E0 = _mm_sha1nexte_epu32(E0, MSG2);
    E1 = ABCD;
    MSG3 = _mm_sha1msg2_epu32(MSG3, MSG2);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E0, 2);
    MSG1 = _mm_sha1msg1_epu32(MSG1, MSG2);
    MSG0 = _mm_xor_si128(MSG0, MSG2);

    // Rounds 44-47
    E1 = _mm_sha1nexte_epu32(E1, MSG3);
    E0 = ABCD;
    MSG0 = _mm_sha1msg2_epu32(MSG0, MSG3);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E1, 2);
    MSG2 = _mm_sha1msg1_epu32(MSG2, MSG3);
    MSG1 = _mm_xor_si128(MSG1, MSG3);

    // Rounds 48-51
    E0 = _mm_sha1nexte_epu32(E0, MSG0);
    E1 = ABCD;
    MSG1 = _mm_sha1msg2_epu32(MSG1, MSG0);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E0, 2);
    MSG3 = _mm_sha1msg1_epu32(MSG3, MSG0);
    MSG2 = _mm_xor_si128(MSG2, MSG0);

    // Rounds 52-55
    E1 = _mm_sha1nexte_epu32(E1, MSG1);
    E0 = ABCD;
    MSG2 = _mm_sha1msg2_epu32(MSG2, MSG1);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E1, 2);
    MSG0 = _mm_sha1msg1_epu32(MSG0, MSG1);
    MSG3 = _mm_xor_si128(MSG3, MSG1);

    // Rounds 56-59
    E0 = _mm_sha1nexte_epu32(E0, MSG2);
    E1 = ABCD;
    MSG3 = _mm_sha1msg2_epu32(MSG3, MSG2);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E0, 2);
    MSG1 = _mm_sha1msg1_epu32(MSG1, MSG2);
    MSG0 = _mm_xor_si128(MSG0, MSG2);

    // Rounds 60-63
    E1 = _mm_sha1nexte_epu32(E1, MSG3);
    E0 = ABCD;
    MSG0 = _mm_sha1msg2_epu32(MSG0, MSG3);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E1, 3);
    MSG2 = _mm_sha1msg1_epu32(MSG2, MSG3);
    MSG1 = _mm_xor_si128(MSG1, MSG3);

    // Rounds 64-67
    E0 = _mm_sha1nexte_epu32(E0, MSG0);
    E1 = ABCD;
    MSG1 = _mm_sha1msg2_epu32(MSG1, MSG0);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E0, 3);
    MSG3 = _mm_sha1msg1_epu32(MSG3, MSG0);
    MSG2 = _mm_xor_si128(MSG2, MSG0);

    // Rounds 68-71
    E1 = _mm_sha1nexte_epu32(E1, MSG1);
    E0 = ABCD;
    MSG2 = _mm_sha1msg2_epu32(MSG2, MSG1);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E1, 3);
    MSG3 = _mm_xor_si128(MSG3, MSG1);

    // Rounds 72-75
    E0 = _mm_sha1nexte_epu32(E0, MSG2);
    E1 = ABCD;
    MSG3 = _mm_sha1msg2_epu32(MSG3, MSG2);
    ABCD = _mm_sha1rnds4_epu32(ABCD, E0, 3);

    // Rounds 76-79
    E1 = _mm_sha1nexte_epu32(E1, MSG3);
    E0 = ABCD;
    ABCD = _mm_sha1rnds4_epu32(ABCD, E1, 3);

    // Add values back to state
    E0 = _mm_sha1nexte_epu32(E0, E0_SAVE);
    ABCD = _mm_add_epi32(ABCD, ABCD_SAVE);

    // Save state
    ABCD = _mm_shuffle_epi32(ABCD, 0x1B);
    _mm_storeu_si128((__m128i*) state, ABCD);
    *(state+4) = _mm_extract_epi32(E0, 3);
}

void write_callback(void *cb_arg, enum write_err error) {
    if (error != WRITE_SUCCESSFUL) {
        printf("GOT ERROR: %d\n", error);
        return;
    }
}

struct read_cb_data {
    db_data key;
    db_data expected_value;
};

void read_cb(void *cb_arg, enum read_err, db_data value) {
    struct read_cb_data *data = cb_arg;
    if (read_err != READ_SUCCESSFUL) {
        printf("GOT ERROR READING: %d\n", read_err);
        return;
    }

    if (expected_value.length != value.length) {
        printf("Expected length would be %d but was %d for key %s\n", data -> expected_value.length, value.length, data -> key.data);
        return;
    }

    if (memcmp(value.data, expected_value.data, value.length) != 0) {
        printf("Expected data not what was received for key %s\n", data -> key.data);
        return;
    }
}

char *random_bytes(int num_bytes) {
    unsigned int num_ints = (num_bytes>>2) + ((num_bytes&3) ? 1 : 0);
    int *buf = malloc(num_ints*sizeof(int));
    for (int i = 0; i < num_ints; i++) {
        buf[i] = random();
    }
    return buf;
}

db_data generate_key() {
    unsigned int key_exp = (random()) % 7 + 4; // e.g. 5, so key is 2<<5 bytes = 32.
    unsigned int key_len = (2<<key_exp) + (16-(random()%32));
    return db_data{.length=key_len, .buf=random_bytes(key_len)};
}

db_data generate_data() {
    unsigned int data_exp = (random()) % 9 + 6;
    unsigned int data_len = (2<<data_exp) + (64-(random()%128));
    return db_data{.length=data_len, .buf=random_bytes(data_len)};
}

int main() {
    unsigned int seed = 1234;
    srandom(seed);
    unsigned int num_keys = 10;
    void *db = create_db();
    for (int i = 0; i < num_keys; i++) {
        write_value_async(db, generate_key(), generate_data(), write_callback, NULL);
    }

    poll_db(db);
    usleep(10000);
    poll_db(db);

    srandom(seed);
    for (int i = 0; i < num_keys; i++) {
        db_data key = generate_key();
        db_data value = generate_data();
        struct read_cb_data *data = calloc(sizeof(struct read_cb_data), 1);
        data -> key = key;
        data -> expected_value = value;
        read_value_async(db, key, read_cb, data);
    }
}