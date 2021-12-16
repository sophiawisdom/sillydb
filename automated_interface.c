#include "db_interface.h"

#include <stdlib.h>
#include <stdio.h>


void write_callback(void *cb_arg, enum write_err error) {
    struct read_cb_data *data = cb_arg;

    if (error != WRITE_SUCCESSFUL) {
        printf("GOT ERROR ON WRITE: %d\n", error);
    }
    
    free(data -> key.data);
    free(data -> expected_value.data);
    free(data);
}

struct read_cb_data {
    db_data key;
    db_data expected_value;
};

void read_cb(void *cb_arg, enum read_err error, db_data value) {
    struct read_cb_data *data = cb_arg;
    if (error != READ_SUCCESSFUL) {
        printf("GOT ERROR READING: %d\n", error);
        goto exit;
    }

    if (data -> expected_value.length != value.length) {
        printf("Expected length would be %d but was %d for key %.16s\n", data -> expected_value.length, value.length, data -> key.data);
        goto exit;
    }

    if (memcmp(value.data, data -> expected_value.data, value.length) != 0) {
        printf("Expected data not what was received for key %.16s\n", data -> key.data);
        printf("got: %.16s\n", value.data);
        printf("expected: %.16s\n", data -> expected_value.data);
        goto exit;
    }

exit:
    printf("Read completed with err %d!\n", error);
    free(data -> key.data);
    free(data -> expected_value.data);
    free(data);
}

static short halfbyte(char halfbyte) {
    if (halfbyte < 10) {
        return 48 + halfbyte;
    }
    return 97 + halfbyte-10;
}

static short byte_to_hex(unsigned char byte) {
    short firstletter = halfbyte(byte & 15);
    short secondletter = halfbyte((byte & 240)<<4);
    return (secondletter<<8) + (firstletter);
}

char *random_bytes(int num_bytes) {
    unsigned int num_ints = (num_bytes>>2) + ((num_bytes&3) ? 1 : 0);
    int *buf = malloc(num_ints*sizeof(int));
    for (int i = 0; i < num_ints; i++) {
        int val = random();
        short hexfirst = byte_to_hex(val&255);
        short hexsecond = byte_to_hex((val&65280) >> 8);
        buf[i] = (hexfirst << 16) + hexsecond;
    }
    return buf;
}

db_data generate_key() {
    unsigned int key_exp = (random()) % 7 + 4; // e.g. 5, so key is 2<<5 bytes = 32.
    unsigned int key_len = (2<<key_exp) + (16-(random()%32));
    db_data key = {.length=key_len, .data=random_bytes(key_len)};
    return key;
}

db_data generate_data() {
    unsigned int data_exp = (random() % 9) + 6;
    unsigned int data_len = (2<<data_exp) + (64-(random()%128));
    db_data value = {.length=data_len, .data=random_bytes(data_len)};
    return value;
}

int main() {
    unsigned int seed = 1234;
    unsigned int num_keys = 10;
    void *db = create_db();
    srandom(seed);
    for (int i = 0; i < num_keys; i++) {
        db_data key = generate_key();
        db_data value = generate_data();
        struct read_cb_data *data = calloc(sizeof(struct read_cb_data), 1);
        data -> key = key;
        data -> expected_value = value;
        write_value_async(db, key, value, write_callback, NULL);
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

    poll_db(db);
    usleep(10000);
    poll_db(db);

    printf("Exiting!\n");
}